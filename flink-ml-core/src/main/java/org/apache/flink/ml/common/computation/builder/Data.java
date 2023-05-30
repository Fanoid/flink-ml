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
import org.apache.flink.ml.common.computation.purefunc.IterativeMapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.OneInputPureFunc;
import org.apache.flink.ml.common.computation.purefunc.TwoInputPureFunc;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents a dataset of multiple records of type T.
 *
 * @param <T> The type of record.
 */
public class Data<T> {

    public final TypeInformation<T> type;

    public Data(TypeInformation<T> type) {
        this.type = type;
    }

    public static OutputDataList transform(
            String name,
            Computation computation,
            List<TypeInformation<?>> outTypes,
            Data<?>... inputs) {
        Preconditions.checkArgument(computation.getNumInputs() == inputs.length);
        Preconditions.checkArgument(computation.getNumOutputs() == outTypes.size());
        return new OutputDataList(Arrays.asList(inputs), name, computation, outTypes);
    }

    public PartitionedData<T> rebalance() {
        return new PartitionedData<>(this, PartitionStrategy.REBALANCE);
    }

    public PartitionedData<T> all() {
        return new PartitionedData<>(this, PartitionStrategy.ALL);
    }

    public PartitionedData<T> broadcast() {
        return new PartitionedData<>(this, PartitionStrategy.BROADCAST);
    }

    public <K> PartitionedData<T> groupByKey(KeySelector<T, K> keySelector) {
        return new PartitionedData<>(this, PartitionStrategy.GROUP_BY_KEY, keySelector);
    }

    public SequentialReadData<T> cacheWithSequentialRead() {
        return new SequentialReadData<>(this);
    }

    public RandomReadData<T> cacheWithRandomRead() {
        return new RandomReadData<>(this);
    }

    public RandomReadWriteData<T> cacheWithRandomReadWrite() {
        return new RandomReadWriteData<>(this);
    }

    public <R> Data<R> map(MapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return map(mapper.getClass().getSimpleName(), mapper, outType);
    }

    public <R> Data<R> map(String name, MapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transformOneInputPureFunc(name, mapper, outType);
    }

    public <R> Data<R> map(IterativeMapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return map(mapper.getClass().getSimpleName(), mapper, outType);
    }

    public <R> Data<R> map(
            String name, IterativeMapPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transformOneInputPureFunc(name, mapper, outType);
    }

    public <R, DATA> Data<R> map(
            MapWithDataPureFunc<T, DATA, R> mapper, Data<DATA> data, TypeInformation<R> outType) {
        return map(mapper.getClass().getSimpleName(), mapper, data, outType);
    }

    public <R, DATA> Data<R> map(
            String name,
            MapWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return transformTwoInputPureFunc(name, mapper, outType, data);
    }

    public <R> Data<R> mapPartition(MapPartitionPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return mapPartition(mapper.getClass().getSimpleName(), mapper, outType);
    }

    public <R> Data<R> mapPartition(
            String name, MapPartitionPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transformOneInputPureFunc(name, mapper, outType);
    }

    public <R, DATA> Data<R> mapPartition(
            MapPartitionWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return mapPartition(mapper.getClass().getSimpleName(), mapper, data, outType);
    }

    public <R, DATA> Data<R> mapPartition(
            String name,
            MapPartitionWithDataPureFunc<T, DATA, R> mapper,
            Data<DATA> data,
            TypeInformation<R> outType) {
        return transformTwoInputPureFunc(name, mapper, outType, data);
    }

    <R> Data<R> transformOneInputPureFunc(
            String name, OneInputPureFunc<T, R> mapper, TypeInformation<R> outType) {
        return transform(
                        name,
                        new PureFuncComputation(mapper),
                        Collections.singletonList(outType),
                        this)
                .get(0);
    }

    <R, DATA> Data<R> transformTwoInputPureFunc(
            String name,
            TwoInputPureFunc<T, DATA, R> mapper,
            TypeInformation<R> outType,
            Data<DATA> data) {
        return transform(
                        name,
                        new PureFuncComputation(mapper),
                        Collections.singletonList(outType),
                        this,
                        data)
                .get(0);
    }

    public TypeInformation<T> getType() {
        return type;
    }
}
