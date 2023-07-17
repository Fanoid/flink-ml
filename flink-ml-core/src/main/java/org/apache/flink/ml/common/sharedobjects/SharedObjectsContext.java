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

package org.apache.flink.ml.common.sharedobjects;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.function.BiConsumerWithException;

/**
 * Context for shared objects. Every operator implementing {@link SharedObjectsStreamOperator} will
 * get an instance of this context set by {@link
 * SharedObjectsStreamOperator#onSharedObjectsContextSet} in runtime. User-defined logic can be
 * invoked through {@link #invoke} with the access (get/set) to shared items.
 *
 * <p>The order of `get` and `set` to a same shared item is strictly controlled with
 * `epochWatermark` of {@link org.apache.flink.iteration.IterationListener}. Every call of `set`
 * brings the epoch watermark of the caller. Then every call of `get` must also bring the epoch
 * watermark it expects to fetch. If these two epoch watermarks match, the `get` returns values
 * without waiting. If the epoch watermark of `get` is smaller than that of `set`, the call of `get`
 * is failed. If the epoch watermark of `get` is larger than that of `set`, the call of `get` can
 * wait for the correct value.
 */
@Experimental
public interface SharedObjectsContext {

    /**
     * Invoke user defined function with provided getters/setters of the shared objects.
     *
     * @param func User defined function where share items can be accessed through getters/setters.
     * @throws Exception Possible exception.
     */
    void invoke(BiConsumerWithException<SharedItemGetter, SharedItemSetter, Exception> func)
            throws Exception;

    /** Interface of shared item getter. */
    interface SharedItemGetter {

        /**
         * Get the value of the shared object identified by `key` with current epoch watermark plus
         * an offset.
         *
         * @param key The key of the shared object.
         * @param offset The offset to current epoch watermark.
         * @return The value of the shared object.
         * @param <T> The type of the shared object.
         */
        <T> T get(ItemDescriptor<T> key, int offset);

        /**
         * Get the value of the shared object identified by `key` with current epoch watermark.
         *
         * @param key The key of the shared object.
         * @return The value of the shared object.
         * @param <T> The type of the shared object.
         */
        default <T> T get(ItemDescriptor<T> key) {
            return get(key, 0);
        }
    }

    /** Interface of shared item writer. */
    interface SharedItemSetter {
        /**
         * Set the shared object identified by `key` to `value` with current epoch watermark.
         *
         * @param key The key of the shared object.
         * @param value The value to be set.
         * @param <T> The type of the shared object.
         */
        <T> void set(ItemDescriptor<T> key, T value);

        /**
         * Renew the shared object identified by `key` with current epoch watermark.
         *
         * @param key The key of the shared object.
         * @param <T> The type of the shared object.
         */
        <T> void renew(ItemDescriptor<T> key);
    }
}
