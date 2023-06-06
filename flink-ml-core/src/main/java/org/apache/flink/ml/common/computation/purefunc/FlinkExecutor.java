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

package org.apache.flink.ml.common.computation.purefunc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/** ... */
public class FlinkExecutor {
    public static <IN, OUT> DataStream<OUT> execute(
            DataStream<IN> in, MapPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMap",
                outType,
                new ExecutorMapPureFuncOperator<>(func, in.getParallelism()));
    }

    private static class ExecutorMapPureFuncOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
        private final MapPureFunc<IN, OUT> func;
        private final int inputParallelism;

        private transient Collector<OUT> collector;

        private transient StateHandler stateHandler;

        public ExecutorMapPureFuncOperator(MapPureFunc<IN, OUT> func, int inputParallelism) {
            this.func = func;
            this.inputParallelism = inputParallelism;
        }

        @Override
        public void open() throws Exception {
            super.open();
            if (func instanceof RichMapPureFunc) {
                ((RichMapPureFunc<IN, OUT>) func)
                        .setContext(
                                new PureFuncContextImpl(
                                        getRuntimeContext().getNumberOfParallelSubtasks(),
                                        getRuntimeContext().getIndexOfThisSubtask(),
                                        inputParallelism));
                ((RichMapPureFunc<IN, OUT>) func).open();
            }
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            func.map(element.getValue(), collector);
        }

        @Override
        public void endInput() throws Exception {
            if (func instanceof RichMapPureFunc) {
                ((RichMapPureFunc<IN, OUT>) func).close(collector);
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            if (func instanceof RichMapPureFunc) {
                stateHandler = new StateHandler(((RichMapPureFunc<IN, OUT>) func).getStateDescs());
                stateHandler.initializeState(context);
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            if (func instanceof RichMapPureFunc) {
                stateHandler.snapshotState(context);
            }
        }
    }
}
