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

package org.apache.flink.ml.common.computation.execution;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;

/** ... */
@SuppressWarnings({"rawtypes", "unchecked"})
public class FlinkIterationExecutor implements ComputationExecutor<DataStream> {
    private static final FlinkIterationExecutor instance = new FlinkIterationExecutor();

    public static FlinkIterationExecutor getInstance() {
        return instance;
    }

    @Override
    public <IN, OUT> DataStream<OUT> executeMap(
            DataStream in, MapPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMap",
                outType,
                new FlinkExecutorUtils.ExecuteMapPureFuncOperator(func, in.getParallelism()));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapWithData(
            DataStream in,
            DataStream data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapWithData",
                        outType,
                        new FlinkExecutorUtils.ExecuteMapWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }

    @Override
    public <IN, OUT> DataStream executeMapPartition(
            DataStream in, MapPartitionPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMapPartition",
                outType,
                new FlinkExecutorUtils.ExecutorMapPartitionPureFuncOperator<>(
                        func, in.getParallelism()));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapPartitionWithData(
            DataStream in,
            DataStream data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapPartitionWithData",
                        outType,
                        new FlinkExecutorUtils.ExecuteMapPartitionWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }

    @Override
    public <OUT> DataStream<OUT> executeOtherPureFunc(
            List<DataStream> inputs, PureFunc<OUT> func, TypeInformation<OUT> outType) {
        return (DataStream<OUT>) func.executeOnFlink((List<DataStream<?>>) (List) inputs).get(0);
    }

    @Override
    public List<DataStream> execute(CompositeComputation computation, List<DataStream> inputs)
            throws Exception {
        return null;
    }

    @Override
    public List<DataStream> execute(Computation computation, List<DataStream> inputs) {
        return null;
    }
}
