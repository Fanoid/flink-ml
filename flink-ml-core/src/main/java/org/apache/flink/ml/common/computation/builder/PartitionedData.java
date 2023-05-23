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

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Represents a dataset with partition strategy.
 *
 * @param <T> The type of record.
 */
public class PartitionedData<T> extends Data<T> {
    public final PartitionStrategy partitionStrategy;

    private final Data<T> upstream;
    private final KeySelector<T, ?> keySelector;

    PartitionedData(Data<T> data, PartitionStrategy partitionStrategy) {
        this(data, partitionStrategy, null);
    }

    <K> PartitionedData(
            Data<T> data, PartitionStrategy partitionStrategy, KeySelector<T, K> keySelector) {
        super(data.type);
        this.upstream = data;
        this.partitionStrategy = partitionStrategy;
        this.keySelector = keySelector;
    }
}
