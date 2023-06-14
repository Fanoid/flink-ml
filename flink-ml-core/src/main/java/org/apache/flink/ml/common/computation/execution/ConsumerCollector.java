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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.function.Consumer;

@Internal
public class ConsumerCollector<T> implements Collector<T> {
    private final SerializedConsumer<T> consumer;

    public ConsumerCollector(SerializedConsumer<T> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void collect(T record) {
        consumer.accept(record);
    }

    @Override
    public void close() {}

    /**
     * Serialized version of {@link Consumer}.
     *
     * @param <T> The type of value to be consumed.
     */
    interface SerializedConsumer<T> extends Serializable, Consumer<T> {}
}
