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

package org.apache.flink.ml;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test for final checkpoint. */
public class FinalCheckpointTest {

    private static final Logger LOG = LoggerFactory.getLogger(FinalCheckpointTest.class);

    @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;
    private Table trainData;

    @Before
    public void before() {
        //        env =
        //                StreamExecutionEnvironment.createRemoteEnvironment(
        //                        "localhost",
        //                        8081,
        //
        // "/Users/fanhong/Code/flink-ml/flink-ml-lib/target/flink-ml-lib-2.3-SNAPSHOT-tests.jar");
        env = StreamExecutionEnvironment.createLocalEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().disableGenericTypes();
        env.setParallelism(2);
        env.enableCheckpointing(20 * 1000);
        env.setRestartStrategy(RestartStrategies.noRestart());
        tEnv = StreamTableEnvironment.create(env);
        trainData = tEnv.fromValues(1, 2, 3, 4, 5, 6);
    }

    private static class PassByOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            System.err.printf(
                    "thread: %s, enter PassByOperator#snapshotState%n", Thread.currentThread());
            LOG.info("Thread: {}, PassByOperator#snapshotState", Thread.currentThread());
            super.snapshotState(context);
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            Thread.sleep(1000);
            output.collect(element);
        }
    }

    @Test
    public void testFinalCheckpoint() throws Exception {
        DataStream<Integer> dataStream = tEnv.toDataStream(trainData, Integer.class);
        dataStream = dataStream.transform("pass-by", Types.INT, new PassByOperator());
        dataStream.addSink(
                new SinkFunction<Integer>() {
                    @Override
                    public void invoke(Integer value, Context context) {
                        LOG.info("Sink received: {}", value);
                        System.err.println("Sink received: " + value);
                    }
                });
        env.execute();
    }
}
