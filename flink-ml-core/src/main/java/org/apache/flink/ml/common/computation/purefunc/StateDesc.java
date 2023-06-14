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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.operator.OperatorStateUtils;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;

import java.io.Serializable;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Descriptors for states, which is similar with {@link StateDescriptor}, except snapshot and
 * initialize functions are provided. In this way, the snapshot and initialization of states are
 * only be called by the infrastructure.
 *
 * <p>TODO: currently only operator state is supported.
 *
 * @param <S> The type of {@link State}.
 * @param <T> The type of value stored in the {@link State}.
 */
public class StateDesc<S extends State, T> {

    /** An instance of {@link StateDescriptor}. */
    public final StateDescriptor<S, T> desc;

    /**
     * Snapshot function applied to {@link State}, which is called inside {@link
     * CheckpointedStreamOperator#snapshotState}.
     */
    public final SerializedConsumerWithException<S> snapshotFn;

    /**
     * Initialize function applied to {@link State}, which is called inside {@link
     * CheckpointedStreamOperator#initializeState}.
     */
    public final SerializedConsumerWithException<S> initializeFn;

    private StateDesc(
            StateDescriptor<S, T> desc,
            SerializedConsumerWithException<S> snapshotFn,
            SerializedConsumerWithException<S> initializeFn) {
        this.desc = desc;
        this.snapshotFn = snapshotFn;
        this.initializeFn = initializeFn;
    }

    public static <S extends State, T> StateDesc<S, T> of(
            StateDescriptor<S, T> desc,
            SerializedConsumerWithException<S> snapshotFn,
            SerializedConsumerWithException<S> initializeFn) {
        return new StateDesc<>(desc, snapshotFn, initializeFn);
    }

    /**
     * A convenient function to construct a single-valued {@link StateDesc}.
     *
     * <p>When doing initializing, the value of extracted from the state, and the caller can use the
     * value through the `getter`. If there is no value stored in the state, the default value is
     * used.
     *
     * <p>When doing snapshot, the current value is obtained through the `getter`, and set to the
     * state.
     *
     * @param name name of the state.
     * @param type type information of the data.
     * @param defaultValue default value of the state.
     * @param setter a setter function when the value is fetched from the state.
     * @param getter a getter function to get current value.
     * @return An instance of {@link StateDesc}.
     * @param <T> The type of value.
     */
    public static <T> StateDesc<?, ?> singleValueState(
            String name,
            TypeInformation<T> type,
            T defaultValue,
            Consumer<T> setter,
            Supplier<T> getter) {
        return StateDesc.of(
                new ListStateDescriptor<>(name, type),
                (s) -> {
                    s.clear();
                    T v = getter.get();
                    if (null != v) {
                        s.add(v);
                    }
                },
                (s) -> {
                    T v = OperatorStateUtils.getUniqueElement(s, name).orElse(defaultValue);
                    setter.accept(v);
                });
    }

    /**
     * Serialized version of {@link Consumer} with exceptions.
     *
     * @param <T> The type of value to be consumed.
     */
    public interface SerializedConsumerWithException<T> extends Serializable {
        void accept(T t) throws Exception;
    }
}
