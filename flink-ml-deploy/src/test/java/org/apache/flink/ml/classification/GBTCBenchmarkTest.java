/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.classification;

import org.apache.flink.Case;
import org.apache.flink.GbtTester;
import org.apache.flink.SparseVectorJsonUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifier;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifierModel;
import org.apache.flink.ml.evaluation.binaryclassification.BinaryClassificationEvaluator;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.VectorTypeInfo;
import org.apache.flink.ml.util.JsonUtils;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.Expressions.$;

/** Hello. */
public class GBTCBenchmarkTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();
    private final Mode mode = Mode.TRAIN;
    //    private final String dataDir = "/home/hongfan.hf/workspace/code/data/";
    private final String dataDir = "file:///Users/fanhong/Downloads/";
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @Before
    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        //        env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", 8081, 3);
        env.getConfig().enableObjectReuse();
        env.setParallelism(4);
        env.enableCheckpointing(5000);
        //        env.getCheckpointConfig().disableCheckpointing();
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        new FileSystemCheckpointStorage(
                                "file://" + dataDir + "checkpoint_storage"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);

        tEnv.createTemporaryFunction(
                "deserialize", SparseVectorJsonUtils.DeserializeSparseVector.class);
    }

    private void showAuc(
            Table prediction, String labelCol, String rawPredictionCol, double expectedAuc) {
        BinaryClassificationEvaluator evaluator =
                new BinaryClassificationEvaluator()
                        .setLabelCol(labelCol)
                        .setRawPredictionCol(rawPredictionCol);
        Table evalResults = evaluator.transform(prediction)[0];

        //noinspection unchecked
        List<Row> rows = IteratorUtils.toList(evalResults.execute().collect());
        System.out.println(rows);
        for (Row row : rows) {
            double auc = row.getFieldAs(0);
            System.out.println("AUC: " + auc);
            Assert.assertTrue(auc > expectedAuc);
        }
    }

    private void showElapsedTime(Table t) {
        tEnv.toDataStream(t).addSink(new DiscardingSink<>());
        long start = System.currentTimeMillis();
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
    }

    private void testing(
            GBTClassifier gbtc, Table trainDataTable, Table testDataTable, double expectedAuc) {
        GBTClassifierModel model = gbtc.fit(trainDataTable);

        if (Mode.TRAIN.equals(mode)) {
            showElapsedTime(model.getModelData()[0]);
        } else if (Mode.TRAIN_AND_TEST.equals(mode)) {
            Table prediction =
                    model.transform(testDataTable)[0].select(
                            $(gbtc.getLabelCol()),
                            $(gbtc.getPredictionCol()),
                            $(gbtc.getRawPredictionCol()),
                            $(gbtc.getProbabilityCol()));
            showAuc(prediction, gbtc.getLabelCol(), gbtc.getRawPredictionCol(), expectedAuc);
        }
    }

    private DataStream<Row> stringToRow(DataStream<String> raw) {
        return raw.map(
                d -> {
                    String[] splits = d.split(" ");
                    int[] keys = new int[splits.length - 1];
                    double[] values = new double[splits.length - 1];
                    for (int i = 1; i < splits.length; i += 1) {
                        String[] kv = splits[i].split(":");
                        keys[i - 1] = Integer.parseInt(kv[0]);
                        values[i - 1] = Double.parseDouble(kv[1]);
                    }
                    final int vectorSize = 128;
                    assert keys[keys.length - 1] <= vectorSize;
                    return Row.of(Double.parseDouble(splits[0]), Vectors.sparse(128, keys, values));
                },
                Types.ROW_NAMED(
                        new String[] {"label", "vec"}, Types.DOUBLE, VectorTypeInfo.INSTANCE));
    }

    @Test
    public void testAgaricus() {
        String trainFile = dataDir + "agaricus.txt.train.txt";
        String testFile = dataDir + "agaricus.txt.test.txt";
        Table trainDataTable = tEnv.fromDataStream(stringToRow(env.readTextFile(trainFile)));
        Table testDataTable = tEnv.fromDataStream(stringToRow(env.readTextFile(testFile)));

        GBTClassifier gbtc =
                new GBTClassifier()
                        .setFeaturesCols("vec")
                        .setLabelCol("label")
                        .setPredictionCol("pred")
                        .setMaxBins(2)
                        .setMaxDepth(6)
                        .setMaxIter(10)
                        .setFeatureSubsetStrategy("all")
                        .setStepSize(.3)
                        .setMinInstancesPerNode(100)
                        .setRegGamma(0.);
        testing(gbtc, trainDataTable, testDataTable, 0.999);
    }

    @Test
    public void testAdult() throws Exception {
        int[] categoricalColIndices = new int[] {1, 3, 5, 6, 7, 8, 9, 13};
        Case c = new Case();
        c.dataDir = dataDir;
        c.trainFile = "adult_train.double_label.csv";
        c.testFile = "adult_test.double_label.csv";
        c.colNames =
                new String[] {
                    "age",
                    "workclass",
                    "fnlwgt",
                    "education",
                    "education_num",
                    "marital_status",
                    "occupation",
                    "relationship",
                    "race",
                    "sex",
                    "capital_gain",
                    "capital_loss",
                    "hours_per_week",
                    "native_country",
                    "label"
                };
        c.colTypeStrs =
                new String[] {
                    "LONG", "STRING", "LONG", "STRING", "LONG", "STRING", "STRING", "STRING",
                    "STRING", "STRING", "LONG", "LONG", "LONG", "STRING", "DOUBLE"
                };
        c.needDeserializeVector = false;

        c.labelCol = "label";
        c.featureCols = ArrayUtils.removeElements(c.colNames, "label");
        c.categoricalCols =
                Arrays.stream(categoricalColIndices)
                        .mapToObj(d -> c.featureCols[d])
                        .toArray(String[]::new);
        c.maxDepth = 6;
        c.maxIter = 10;

        c.mode = Case.Mode.TRAIN;
        c.expectedAuc = 0.907;

        c.parallelism = 4;
        c.checkpointInterval = 5000;
        c.checkpointStorage = dataDir + "checkpoint_storage";

        System.out.println(JsonUtils.OBJECT_MAPPER.writeValueAsString(c));
        GbtTester.testCase(c);
    }

    @Test
    public void testAvazu() throws Exception {
        Case c = new Case();
        c.dataDir = dataDir;
        c.trainFile = "avazu-ctr-prediction/train_1M.csv";
        c.testFile = "avazu-ctr-prediction/test_10w.csv";
        c.colNames =
                new String[] {
                    "id",
                    "click",
                    "hour",
                    "C1",
                    "banner_pos",
                    "site_id",
                    "site_domain",
                    "site_category",
                    "app_id",
                    "app_domain",
                    "app_category",
                    "device_id",
                    "device_ip",
                    "device_model",
                    "device_type",
                    "device_conn_type",
                    "C14",
                    "C15",
                    "C16",
                    "C17",
                    "C18",
                    "C19",
                    "C20",
                    "C21"
                };
        c.colTypeStrs =
                new String[] {
                    "STRING", "DOUBLE", "STRING", "STRING", "INT", "STRING", "STRING", "STRING",
                    "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING",
                    "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT",
                };
        c.needDeserializeVector = false;

        c.labelCol = "click";
        c.featureCols = ArrayUtils.removeElements(c.colNames, "id", "click");
        c.categoricalCols = c.featureCols;
        c.maxDepth = 6;
        c.maxIter = 10;

        c.mode = Case.Mode.TRAIN;

        c.parallelism = 4;
        c.checkpointInterval = -1;
        c.checkpointStorage = dataDir + "checkpoint_storage";

        System.out.println(JsonUtils.OBJECT_MAPPER.writeValueAsString(c));
        GbtTester.testCase(c);
    }

    @Test
    public void testSV() throws Exception {
        Case c = new Case();
        c.dataDir = dataDir;
        c.trainFile = "sv10w_3k.csv";
        c.testFile = "sv1w_3k.csv";
        c.colNames = new String[] {"features", "label"};
        c.colTypeStrs = new String[] {"STRING", "DOUBLE"};
        c.needDeserializeVector = true;

        c.labelCol = "label";
        c.featureCols = new String[] {"features"};
        c.maxDepth = 6;
        c.maxIter = 10;

        c.mode = Case.Mode.TRAIN_AND_TEST;

        c.parallelism = 4;
        c.checkpointInterval = -1;
        c.checkpointStorage = dataDir + "checkpoint_storage";

        System.out.println(JsonUtils.OBJECT_MAPPER.writeValueAsString(c));
        GbtTester.testCase(c);
    }

    @Test
    public void testCriteoNumerical() throws Exception {
        String[] categoricalCols =
                new String[] {
                    "cf01", "cf02", "cf03", "cf04", "cf05", "cf06", "cf07", "cf08", "cf09", "cf10",
                    "cf11", "cf12", "cf13", "cf14", "cf15", "cf16", "cf17", "cf18", "cf19", "cf20",
                    "cf21", "cf22", "cf23", "cf24", "cf25", "cf26"
                };

        String[] numericalCols =
                new String[] {
                    "nf01", "nf02", "nf03", "nf04", "nf05", "nf06", "nf07", "nf08", "nf09", "nf10",
                    "nf11", "nf12", "nf13"
                };

        String[] colNames = new String[] {"label"};
        colNames = ArrayUtils.addAll(colNames, numericalCols);
        colNames = ArrayUtils.addAll(colNames, categoricalCols);
        TypeInformation<?>[] colTypes = new TypeInformation[] {Types.DOUBLE};
        colTypes =
                ArrayUtils.addAll(
                        colTypes,
                        Collections.nCopies(numericalCols.length, Types.DOUBLE)
                                .toArray(new TypeInformation[0]));
        colTypes =
                ArrayUtils.addAll(
                        colTypes,
                        Collections.nCopies(categoricalCols.length, Types.STRING)
                                .toArray(new TypeInformation[0]));

        Case c = new Case();
        c.dataDir = dataDir;
        c.trainFile = "criteo_500w.csv";
        c.testFile = "xxx";
        c.colNames = colNames;
        c.colTypeStrs =
                Arrays.stream(colTypes).map(d -> d.toString().toUpperCase()).toArray(String[]::new);
        c.needDeserializeVector = false;

        String schemaStr =
                IntStream.range(0, c.colNames.length)
                        .mapToObj(d -> c.colNames[d] + " " + c.colTypeStrs[d])
                        .collect(Collectors.joining(", "));
        System.out.println(schemaStr);

        c.labelCol = "label";
        c.featureCols = numericalCols;
        c.maxDepth = 6;
        c.maxIter = 10;

        c.mode = Case.Mode.TRAIN_AND_TEST;

        c.parallelism = 4;
        c.checkpointInterval = -1;
        c.checkpointStorage = dataDir + "checkpoint_storage";

        System.out.println(JsonUtils.OBJECT_MAPPER.writeValueAsString(c));
        //        GbtTester.testCase(c);
    }

    private enum Mode {
        TRAIN,
        TRAIN_AND_TEST
    }
}
