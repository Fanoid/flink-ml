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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifier;
import org.apache.flink.ml.classification.gbtclassifier.GBTClassifierModel;
import org.apache.flink.ml.common.gbt.GBTModelDataUtil;
import org.apache.flink.ml.evaluation.binaryclassification.BinaryClassificationEvaluator;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.utils.TypeStringUtils;
import org.apache.flink.types.Row;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Test a GBT {@link Case}. */
public class GbtTester {
    static void showAuc(
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
            if (auc < expectedAuc) {
                throw new RuntimeException(
                        String.format("AUC %f is less then expected %f.", auc, expectedAuc));
            }
        }
    }

    static Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> createEnvs(Case c) {
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.getConfig().enableObjectReuse();
        env.setParallelism(c.parallelism);
        if (c.checkpointInterval > 0) {
            env.enableCheckpointing(c.checkpointInterval);
            env.getCheckpointConfig()
                    .setCheckpointStorage(new FileSystemCheckpointStorage(c.checkpointStorage));
        } else {
            env.getCheckpointConfig().disableCheckpointing();
        }
        env.setRestartStrategy(RestartStrategies.noRestart());
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporaryFunction(
                "deserialize", SparseVectorJsonUtils.DeserializeSparseVector.class);
        return Tuple2.of(env, tEnv);
    }

    static void testCaseWithEnv(Case c, StreamExecutionEnvironment env, StreamTableEnvironment tEnv)
            throws Exception {
        //noinspection deprecation
        TypeInformation<?>[] colTypes =
                Arrays.stream(c.colTypeStrs)
                        .map(TypeStringUtils::readTypeInfo)
                        .toArray(TypeInformation[]::new);

        Table trainDataTable =
                tEnv.fromDataStream(
                        env.readFile(
                                        new RowCsvInputFormat(
                                                new Path(c.dataDir + c.trainFile), colTypes),
                                        c.dataDir + c.trainFile)
                                .returns(Types.ROW_NAMED(c.colNames, colTypes)));
        if (c.needDeserializeVector) {
            //noinspection deprecation
            trainDataTable =
                    trainDataTable.select(
                            String.format(
                                    "deserialize(%s, %d) as %s, %s",
                                    c.featureCols[0], c.vectorSize, c.featureCols[0], c.labelCol));
        }

        GBTClassifier gbtc =
                new GBTClassifier()
                        .setFeaturesCols(c.featureCols)
                        .setLabelCol(c.labelCol)
                        .setMaxDepth(c.maxDepth)
                        .setMaxIter(c.maxIter)
                        .setPredictionCol("pred")
                        .setFeatureSubsetStrategy("all")
                        .setStepSize(.3)
                        .setMaxBins(128)
                        .setMinInstancesPerNode(100)
                        .setRegGamma(0.)
                        .setBaseScore(0.5);
        if (ArrayUtils.isNotEmpty(c.categoricalCols)) {
            gbtc = gbtc.setCategoricalCols(c.categoricalCols);
        }

        GBTClassifierModel model = gbtc.fit(trainDataTable);

        if (c.mode.equals(Case.Mode.TRAIN)) {
            if (StringUtils.isEmpty(c.modelFile)) {
                GBTModelDataUtil.getModelDataStream(model.getModelData()[0])
                        .addSink(new DiscardingSink<>());
            } else {
                Path modelFilePath = new Path(c.dataDir + c.modelFile);
                modelFilePath.getFileSystem().delete(modelFilePath, true);
                model.save(c.dataDir + c.modelFile);
            }
            long start = System.currentTimeMillis();
            try {
                env.execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            System.out.println("Elapsed time: " + (System.currentTimeMillis() - start));
        } else {
            Table testDataTable =
                    tEnv.fromDataStream(
                            env.readFile(
                                            new RowCsvInputFormat(
                                                    new Path(c.dataDir + c.testFile), colTypes),
                                            c.dataDir + c.testFile)
                                    .returns(Types.ROW_NAMED(c.colNames, colTypes)));
            if (c.needDeserializeVector) {
                //noinspection deprecation
                testDataTable =
                        testDataTable.select(
                                String.format(
                                        "deserialize(%s, %d) as %s, %s",
                                        c.featureCols[0],
                                        c.vectorSize,
                                        c.featureCols[0],
                                        c.labelCol));
            }
            Table prediction =
                    model.transform(testDataTable)[0].select(
                            $(gbtc.getLabelCol()),
                            $(gbtc.getPredictionCol()),
                            $(gbtc.getRawPredictionCol()),
                            $(gbtc.getProbabilityCol()));
            showAuc(prediction, gbtc.getLabelCol(), gbtc.getRawPredictionCol(), c.expectedAuc);
        }
    }

    public static void testCase(Case c) throws Exception {
        Tuple2<StreamExecutionEnvironment, StreamTableEnvironment> envs = createEnvs(c);
        testCaseWithEnv(c, envs.f0, envs.f1);
    }
}
