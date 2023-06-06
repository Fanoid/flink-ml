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
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecutorMapPartitionPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutorUtils.ExecutorMapPureFuncOperator;
import org.apache.flink.streaming.api.datastream.DataStream;

/** ... */
public class FlinkExecutor {
    public static <IN, OUT> DataStream<OUT> execute(
            DataStream<IN> in, MapPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMap",
                outType,
                new ExecutorMapPureFuncOperator<>(func, in.getParallelism()));
    }

    public static <IN, OUT> DataStream<OUT> execute(
            DataStream<IN> in, MapPartitionPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMapPartition",
                outType,
                new ExecutorMapPartitionPureFuncOperator<>(func, in.getParallelism()));
    }
}
