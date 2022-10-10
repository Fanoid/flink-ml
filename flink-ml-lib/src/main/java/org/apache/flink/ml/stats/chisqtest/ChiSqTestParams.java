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

package org.apache.flink.ml.stats.chisqtest;

import org.apache.flink.ml.common.param.HasFeaturesCol;
import org.apache.flink.ml.common.param.HasLabelCol;
import org.apache.flink.ml.param.BooleanParam;
import org.apache.flink.ml.param.Param;

/**
 * Params for {@link ChiSqTest}.
 *
 * @param <T> The class type of this instance.
 */
public interface ChiSqTestParams<T> extends HasFeaturesCol<T>, HasLabelCol<T> {
    Param<Boolean> FLATTEN =
            new BooleanParam(
                    "flatten",
                    "If false, the returned table contains only a single row, otherwise, one row per feature.",
                    false);

    default boolean getFlatten() {
        return get(FLATTEN);
    }

    default T setFlatten(boolean value) {
        return set(FLATTEN, value);
    }
}
