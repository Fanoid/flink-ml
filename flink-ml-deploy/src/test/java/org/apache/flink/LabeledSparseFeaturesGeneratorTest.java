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
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.ml.benchmark.datagenerator.common.LabeledPointWithWeightGenerator;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.OutputStream;

/** Hello! */
public class LabeledSparseFeaturesGeneratorTest {

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @Before
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.setParallelism(4);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
    }

    private static class CsvEncoder implements Encoder<Row> {
        @Override
        public void encode(Row element, OutputStream stream) throws IOException {
            stream.write((element.getFieldAs(0) + "," + element.getFieldAs(1) + "\n").getBytes());
        }
    }

    @Test
    public void test() throws Exception {
        LabeledSparseFeaturesGenerator generator =
                new LabeledSparseFeaturesGenerator()
                        .setLabelArity(2)
                        .setVectorDim(3000)
                        .setFeatureNnz(3000 / 20)
                        .setNumValues(5000000)
                        .setColNames(new String[] {"features", "label"});
        Table sourceTable = generator.getData(tEnv)[0];
        DataStream<Row> source = tEnv.toDataStream(sourceTable);
        DataStream<Row> transformed =
                source.map(
                        d ->
                                Row.of(
                                        SparseVectorJsonUtils.serialize(d.getFieldAs("features")),
                                        d.getFieldAs("label")));

        FileSink<Row> sink =
                FileSink.forRowFormat(new Path("/tmp/sv.csv"), new CsvEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
        transformed.sinkTo(sink);
        env.execute();
    }

    @Test
    public void testDense() throws Exception {
        LabeledPointWithWeightGenerator generator =
                new LabeledPointWithWeightGenerator()
                        .setVectorDim(100)
                        .setFeatureArity(0)
                        .setLabelArity(2)
                        .setNumValues(5000000)
                        .setColNames(new String[] {"features", "label", "weight"});
        Table sourceTable = generator.getData(tEnv)[0];
        DataStream<Row> source = tEnv.toDataStream(sourceTable);
        DataStream<Row> transformed =
                source.map(
                        d ->
                                Row.of(
                                        SparseVectorJsonUtils.serialize(
                                                ((Vector) d.getFieldAs("features")).toSparse()),
                                        d.getFieldAs("label")));

        FileSink<Row> sink =
                FileSink.forRowFormat(new Path("/tmp/dv.csv"), new CsvEncoder())
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .withBucketAssigner(new BasePathBucketAssigner<>())
                        .build();
        transformed.sinkTo(sink);
        env.execute();
    }
}
