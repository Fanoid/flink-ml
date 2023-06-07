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
import org.apache.flink.ml.common.computation.computation.PureFuncComputation;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.util.Preconditions;

import java.util.List;

/** Executor for computations. */
interface ComputationExecutor<T> {
    <IN, OUT> T executeMap(
            T in, MapPureFunc<IN, OUT> func, String name, TypeInformation<OUT> outType);

    <IN, DATA, OUT> T executeMapWithData(
            T in,
            T data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType);

    <IN, OUT> T executeMapPartition(
            T in, MapPartitionPureFunc<IN, OUT> func, String name, TypeInformation<OUT> outType);

    <IN, DATA, OUT> T executeMapPartitionWithData(
            T in,
            T data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType);

    <OUT> T executeOtherPureFunc(
            List<T> inputs, PureFunc<OUT> func, String name, TypeInformation<OUT> outType)
            throws Exception;

    @SuppressWarnings({"unchecked", "rawtypes"})
    default T execute(PureFuncComputation computation, List<T> inputs) throws Exception {
        PureFunc<?> func = computation.getFunc();
        String name = computation.getName();
        TypeInformation<?> outType = computation.getOutTypes().get(0);
        if (func instanceof MapPureFunc) {
            Preconditions.checkArgument(1 == inputs.size());
            return (T) executeMap(inputs.get(0), (MapPureFunc) func, name, outType);
        } else if (func instanceof MapPartitionPureFunc) {
            Preconditions.checkArgument(1 == inputs.size());
            return (T)
                    executeMapPartition(inputs.get(0), (MapPartitionPureFunc) func, name, outType);
        } else if (func instanceof MapWithDataPureFunc) {
            Preconditions.checkArgument(2 == inputs.size());
            return (T)
                    executeMapWithData(
                            inputs.get(0),
                            inputs.get(1),
                            (MapWithDataPureFunc) func,
                            name,
                            outType);
        } else if (func instanceof MapPartitionWithDataPureFunc) {
            Preconditions.checkArgument(2 == inputs.size());
            return (T)
                    executeMapPartitionWithData(
                            inputs.get(0),
                            inputs.get(1),
                            (MapPartitionWithDataPureFunc) func,
                            name,
                            outType);
        } else {
            return (T) executeOtherPureFunc(inputs, (PureFunc) func, name, outType);
        }
    }

    List<T> execute(CompositeComputation computation, List<T> inputs) throws Exception;

    List<T> execute(Computation computation, List<T> inputs);
}
