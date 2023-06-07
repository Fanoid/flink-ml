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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.ml.common.computation.execution.FlinkExecutor;
import org.apache.flink.ml.common.computation.execution.FlinkIterationExecutor;
import org.apache.flink.ml.common.computation.execution.IterableExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;

/**
 * Similar to {@link MapFunction} but with an addition broadcast parameter. Compared to {@link
 * RichMapFunction} with {@link RuntimeContext#getBroadcastVariable}, this interface can be used in
 * a broader situations since it involves no Flink runtime.
 *
 * @param <IN> Type of input elements.
 * @param <OUT> Type of output elements.
 */
@Experimental
@FunctionalInterface
public interface MapPartitionPureFunc<IN, OUT> extends OneInputPureFunc<IN, OUT> {
    void map(Iterable<IN> values, Collector<OUT> out);

    @Override
    default List<Iterable<?>> execute(List<Iterable<?>> inputs) {
        Preconditions.checkArgument(getNumInputs() == inputs.size());
        return Collections.singletonList(
                IterableExecutor.getInstance()
                        .executeMapPartition(
                                inputs.get(0), this, getClass().getSimpleName(), null));
    }

    @Override
    default List<DataStream<?>> executeOnFlink(List<DataStream<?>> inputs) {
        Preconditions.checkArgument(getNumInputs() == inputs.size());
        //noinspection unchecked
        return Collections.singletonList(
                FlinkExecutor.getInstance()
                        .executeMapPartition(
                                inputs.get(0), this, getClass().getSimpleName(), null));
    }

    @Override
    default List<DataStream<?>> executeInIterations(List<DataStream<?>> inputs) {
        Preconditions.checkArgument(getNumInputs() == inputs.size());
        //noinspection unchecked
        return Collections.singletonList(
                FlinkIterationExecutor.getInstance()
                        .executeMapPartition(
                                inputs.get(0), this, getClass().getSimpleName(), null));
    }
}
