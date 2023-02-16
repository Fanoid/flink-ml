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

package org.apache.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifier;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifierModel;
import org.apache.flink.ml.util.JsonUtils;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.table.api.Expressions.$;

/** Entrypoint. */
public class Main {
    private final String dataDir = "oss://alink-test/jiqi-temp/flinkml-gbdt/";
    private Case.Mode mode = Case.Mode.TRAIN;
    private long checkpointInterval = 5000;
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private int parallelism = 4;

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        System.out.println(Arrays.toString(args));
        if (false) {
            main.parallelism = Integer.parseInt(args[0]);
            main.mode = Case.Mode.valueOf(args[1]);
            main.checkpointInterval = Long.parseLong(args[2]);
            main.before();
            //        main.testAvazu1M();
            main.testSV();
            //        main.testCriteo5M();
        } else {
            main.testCase(args[0]);
        }
    }

    public void before() {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.setParallelism(parallelism);
        if (checkpointInterval > 0) {
            env.enableCheckpointing(5000);
            env.getCheckpointConfig()
                    .setCheckpointStorage(
                            new FileSystemCheckpointStorage(dataDir + "checkpoint_storage"));
        } else {
            env.getCheckpointConfig().disableCheckpointing();
        }
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryFunction(
                "deserialize", SparseVectorJsonUtils.DeserializeSparseVector.class);
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

        if (Case.Mode.TRAIN.equals(mode)) {
            showElapsedTime(model.getModelData()[0]);
        } else if (Case.Mode.TRAIN_AND_TEST.equals(mode)) {
            Table prediction =
                    model.transform(testDataTable)[0].select(
                            $(gbtc.getLabelCol()),
                            $(gbtc.getPredictionCol()),
                            $(gbtc.getRawPredictionCol()),
                            $(gbtc.getProbabilityCol()));
            GbtTester.showAuc(
                    prediction, gbtc.getLabelCol(), gbtc.getRawPredictionCol(), expectedAuc);
        }
    }

    public void testAvazu1M() throws Exception {
        String trainFile = dataDir + "train_5M.csv";
        String testFile = dataDir + "test_10w.csv";
        String modelFie = dataDir + "model";
        String[] colNames =
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
        TypeInformation<?>[] colTypes =
                new TypeInformation[] {
                    Types.STRING,
                    Types.DOUBLE,
                    Types.STRING,
                    Types.STRING,
                    Types.INT,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.STRING,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                    Types.INT,
                };

        Table trainDataTable =
                tEnv.fromDataStream(
                        env.readFile(
                                        new RowCsvInputFormat(new Path(trainFile), colTypes),
                                        trainFile)
                                .returns(Types.ROW_NAMED(colNames, colTypes))
                                .rebalance());
        Table testDataTable =
                tEnv.fromDataStream(
                        env.readFile(new RowCsvInputFormat(new Path(testFile), colTypes), testFile)
                                .returns(Types.ROW_NAMED(colNames, colTypes))
                                .rebalance());

        String labelCol = "click";
        String[] featureCols = ArrayUtils.removeElements(colNames, "id", "click");

        GBTClassifier gbtc =
                new GBTClassifier()
                        .setFeaturesCols(featureCols)
                        .setCategoricalCols(featureCols)
                        .setLabelCol(labelCol)
                        .setPredictionCol("pred")
                        .setMaxDepth(5)
                        .setMaxIter(30)
                        .setFeatureSubsetStrategy("all")
                        .setStepSize(.3)
                        .setMinInstancesPerNode(100)
                        .setMaxBins(32)
                        .setRegGamma(0.);
        //        testing(gbtc, trainDataTable, testDataTable, 0.5);
        GBTClassifierModel model = gbtc.fit(trainDataTable);
        Path modelFilePath = new Path(modelFie);
        modelFilePath.getFileSystem().delete(modelFilePath, true);
        model.save(modelFie);
        env.execute();
    }

    public void testCriteo5M() throws Exception {
        String trainFile = dataDir + "criteo_500w.csv";
        String modelFie = dataDir + "criteo_model";

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
        colNames = ArrayUtils.addAll(colNames, categoricalCols);
        colNames = ArrayUtils.addAll(colNames, numericalCols);
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

        Table trainDataTable =
                tEnv.fromDataStream(
                        env.readFile(
                                        new RowCsvInputFormat(new Path(trainFile), colTypes),
                                        trainFile)
                                .returns(Types.ROW_NAMED(colNames, colTypes))
                                .rebalance());

        String labelCol = "label";
        String[] featureCols = ArrayUtils.removeElements(colNames, labelCol);

        GBTClassifier gbtc =
                new GBTClassifier()
                        .setFeaturesCols(featureCols)
                        .setCategoricalCols(featureCols)
                        .setLabelCol(labelCol)
                        .setPredictionCol("pred")
                        .setMaxDepth(5)
                        .setMaxIter(5)
                        .setFeatureSubsetStrategy("all")
                        .setStepSize(.3)
                        .setMinInstancesPerNode(100)
                        .setMaxBins(32)
                        .setRegGamma(0.);
        //        testing(gbtc, trainDataTable, testDataTable, 0.5);
        GBTClassifierModel model = gbtc.fit(trainDataTable);
        Path modelFilePath = new Path(modelFie);
        modelFilePath.getFileSystem().delete(modelFilePath, true);
        model.save(modelFie);
        env.execute();
    }

    public void testSV() throws Exception {
        String trainFile = dataDir + "sv300w_3k.csv";
        String modelFie = dataDir + "sv100w_3k_model";
        String[] colNames = new String[] {"features", "label"};
        TypeInformation<?>[] colTypes = new TypeInformation[] {Types.STRING, Types.DOUBLE};

        Table trainDataTable =
                tEnv.fromDataStream(
                                env.readFile(
                                                new RowCsvInputFormat(
                                                        new Path(trainFile), colTypes),
                                                trainFile)
                                        .returns(Types.ROW_NAMED(colNames, colTypes)))
                        .select("deserialize(features) as features, label");

        String labelCol = "label";
        GBTClassifier gbtc =
                new GBTClassifier()
                        .setFeaturesCols("features")
                        .setLabelCol(labelCol)
                        .setPredictionCol("pred")
                        .setMaxDepth(6)
                        .setMaxIter(50)
                        .setFeatureSubsetStrategy("all")
                        .setStepSize(.3)
                        .setMaxBins(128)
                        .setMinInstancesPerNode(100)
                        .setRegGamma(0.);
        //        testing(gbtc, trainDataTable, testDataTable, 0.5);
        GBTClassifierModel model = gbtc.fit(trainDataTable);
        Path modelFilePath = new Path(modelFie);
        modelFilePath.getFileSystem().delete(modelFilePath, true);
        model.save(modelFie);
        env.execute();
    }

    public void testCase(String path) throws Exception {
        Case c = JsonUtils.OBJECT_MAPPER.readValue(new File(path), Case.class);
        GbtTester.testCase(c);
    }
}
