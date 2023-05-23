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
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;

import java.util.List;

/**
 * A base interface for all rich pure functions. This class defines methods for the life cycle of
 * the functions, as well as methods to access the context in which the functions are executed.
 */
@Experimental
public interface RichPureFunc<OUT> extends PureFunc<OUT> {
    void open() throws Exception;

    void close() throws Exception;

    default void reset() throws Exception {
        close();
        open();
    }

    PureFuncContext getContext();

    void setContext(PureFuncContext context);

    void collect(OUT value);

    List<StateDesc<?, ?>> getStateDescs();

    /**
     * Descriptors for states, which is similar with {@link StateDescriptor}, except snapshot and
     * initialize functions are provided. In this way, the snapshot and initialization of states are
     * only be called by the infrastructure.
     *
     * @param <S> The type of {@link State}.
     * @param <T> The type of value stored in the {@link State}.
     */
    class StateDesc<S extends State, T> {
        public final StateDescriptor<S, T> desc;
        public final SerializedConsumerWithException<S, ?> snapshotFn;
        public final SerializedConsumerWithException<S, ?> initializeFn;

        private StateDesc(
                StateDescriptor<S, T> desc,
                SerializedConsumerWithException<S, ?> snapshotFn,
                SerializedConsumerWithException<S, ?> initializeFn) {
            this.desc = desc;
            this.snapshotFn = snapshotFn;
            this.initializeFn = initializeFn;
        }

        public static <S extends State, T> StateDesc<S, T> of(
                StateDescriptor<S, T> desc,
                SerializedConsumerWithException<S, ?> snapshotFn,
                SerializedConsumerWithException<S, ?> initializeFn) {
            return new StateDesc<>(desc, snapshotFn, initializeFn);
        }
    }
}
