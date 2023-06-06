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
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecuteMapPartitionWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecuteMapPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecuteMapWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecutorMapPartitionPureFuncOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

/** ... */
public class FlinkIterationExecutor {
    public static <IN, OUT> DataStream<OUT> execute(
            DataStream<IN> in, MapPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMap", outType, new ExecuteMapPureFuncOperator<>(func, in.getParallelism()));
    }

    public static <IN, DATA, OUT> DataStream<OUT> execute(
            DataStream<IN> in,
            DataStream<DATA> data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapWithData",
                        outType,
                        new ExecuteMapWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }

    public static <IN, OUT> DataStream<OUT> execute(
            DataStream<IN> in, MapPartitionPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMapPartition",
                outType,
                new ExecutorMapPartitionPureFuncOperator<>(func, in.getParallelism()));
    }

    public static <IN, DATA, OUT> DataStream<OUT> execute(
            DataStream<IN> in,
            DataStream<DATA> data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapPartitionWithData",
                        outType,
                        new ExecuteMapPartitionWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }
}
