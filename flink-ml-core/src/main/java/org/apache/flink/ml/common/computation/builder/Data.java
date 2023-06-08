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

package org.apache.flink.ml.common.computation.builder;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.ml.common.computation.computation.PureFuncComputation;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.OneInputPureFunc;
import org.apache.flink.ml.common.computation.purefunc.ReducePureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichMapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichPureFunc;
import org.apache.flink.ml.common.computation.purefunc.StateDesc;
import org.apache.flink.ml.common.computation.purefunc.TwoInputPureFunc;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Represents a dataset of multiple records of type T.
 *
 * @param <T> The type of records.
 */
public abstract class Data<T> {

    private final TypeInformation<T> type;

    Data(TypeInformation<T> type) {
        this.type = type;
    }

    /**
     * Creates a {@link SourceData}.
     *
     * @param type The type information of records.
     * @return An instance of {@link SourceData}.
     * @param <T> The type of records.
     */
    public static <T> SourceData<T> source(TypeInformation<T> type) {
        return new SourceData<>(type);
    }

    /**
     * Gets the outputs data by applying inputs to a computation.
     *
     * @param computation The computation.
     * @param inputs The inputs.
     * @return The outputs.
     */
    public static OutputDataList transform(Computation computation, Data<?>... inputs) {
        Preconditions.checkArgument(computation.getNumInputs() == inputs.length);
        return new OutputDataList(Arrays.asList(inputs), computation);
    }

    /**
     * Gets the upstreams of current data, i.e. those data being dependent.
     *
     * @return The upstreams.
     */
    public abstract List<Data<?>> getUpstreams();

    /**
     * Applies rebalance strategy to current data.
     *
     * @return The rebalanced data.
     */
    public PartitionedData<T> rebalance() {
        return new PartitionedData<>(this, PartitionedData.PartitionStrategy.REBALANCE);
    }

    /**
     * Applies all strategy to current data.
     *
     * @return The single partitioned data.
     */
    public PartitionedData<T> all() {
        return new PartitionedData<>(this, PartitionedData.PartitionStrategy.ALL);
    }

    /**
     * Applies broadcast strategy to current data.
     *
     * @return The broadcast data.
     */
    public PartitionedData<T> broadcast() {
        return new PartitionedData<>(this, PartitionedData.PartitionStrategy.BROADCAST);
    }

    /**
     * Applies group-by strategy to current data.
     *
     * @param keySelector The function to get key.
     * @return The partitioned data.
     * @param <K> The type of the key.
     */
    public <K> PartitionedData<T> groupByKey(KeySelector<T, K> keySelector) {
        return new PartitionedData<>(
                this, PartitionedData.PartitionStrategy.GROUP_BY_KEY, keySelector);
    }

    /**
     * Gets a cached data with sequential reads.
     *
     * @return The cached data.
     */
    public SequentialReadData<T> cacheWithSequentialRead() {
        return new SequentialReadData<>(this);
    }

    /**
     * Gets a cached data with random reads.
     *
     * @return The cached data.
     */
    public RandomReadData<T> cacheWithRandomRead() {
        return new RandomReadData<>(this);
    }

    /**
     * Gets a cached data with random reads and writes.
     *
     * @return The cached data.
     */
    public RandomReadWriteData<T> cacheWithRandomReadWrite() {
        return new RandomReadWriteData<>(this);
    }

    public <R> Data<R> map(MapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return map(mapper.getClass().getName(), mapper, outType);
    }

    public <R> Data<R> map(String name, MapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transformOneInputPureFunc(name, mapper, outType);
    }

    public <R, DATA> Data<R> map(
            MapWithDataPureFunc<T, DATA, R> mapper, Data<DATA> data, TypeInformation<R> outType) {
        return map(mapper.getClass().getName(), mapper, data, outType);
    }

    public <R, DATA> Data<R> map(
            String name,
            MapWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return transformTwoInputPureFunc(name, mapper, outType, data);
    }

    public <R> Data<R> mapPartition(MapPartitionPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return mapPartition(mapper.getClass().getName(), mapper, outType);
    }

    public <R> Data<R> mapPartition(
            String name, MapPartitionPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transformOneInputPureFunc(name, mapper, outType);
    }

    public <R, DATA> Data<R> mapPartition(
            MapPartitionWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return mapPartition(mapper.getClass().getName(), mapper, data, outType);
    }

    public <R, DATA> Data<R> mapPartition(
            String name,
            MapPartitionWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return transformTwoInputPureFunc(name, mapper, outType, data);
    }

    public Data<T> reduce(ReducePureFunc<T> reducer) {
        return reduce(reducer.getClass().getSimpleName(), reducer);
    }

    public Data<T> reduce(String name, ReducePureFunc<T> reducer) {
        return map(name + "-combine", new MapperForReduce<>(reducer, type), type)
                .all()
                .map(name + "-reduce", new MapperForReduce<>(reducer, type), type);
    }

    <R> Data<R> transformOneInputPureFunc(
            String name, OneInputPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transform(new PureFuncComputation(mapper, name, outType), this).get(0);
    }

    <R, DATA> Data<R> transformTwoInputPureFunc(
            String name,
            TwoInputPureFunc<T, DATA, R> mapper,
            TypeInformation<R> outType,
            Data<DATA> data) {
        return transform(new PureFuncComputation(mapper, name, outType), this, data).get(0);
    }

    public TypeInformation<T> getType() {
        return type;
    }

    static class MapperForReduce<T> extends RichMapPureFunc<T, T> {

        private final ReducePureFunc<T> reducer;
        private final TypeInformation<T> type;

        private T reduced;

        MapperForReduce(ReducePureFunc<T> reducer, TypeInformation<T> type) {
            this.reducer = reducer;
            this.type = type;
        }

        @Override
        public void open() throws Exception {
            reduced = null;
        }

        @Override
        public void close(Collector<T> out) throws Exception {
            if (null != reduced) {
                out.collect(reduced);
            }
        }

        @Override
        public void map(T value, Collector<T> out) throws Exception {
            if (null == reduced) {
                reduced = value;
            } else {
                reduced = reducer.reduce(reduced, value);
            }
        }

        @Override
        public List<StateDesc<?, ?>> getStateDescs() {
            List<StateDesc<?, ?>> stateDescs = new ArrayList<>();
            StateDesc<?, ?> reducedStateDesc =
                    StateDesc.singleValueState(
                            "__reduced_value", type, null, (v) -> reduced = v, () -> reduced);
            stateDescs.add(reducedStateDesc);
            if (reduced instanceof RichPureFunc) {
                stateDescs.addAll(((RichPureFunc<?>) reduced).getStateDescs());
            }
            return stateDescs;
        }
    }
}
