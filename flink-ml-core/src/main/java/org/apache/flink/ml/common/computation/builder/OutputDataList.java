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
import org.apache.flink.ml.common.computation.computation.Computation;

import java.util.List;

/** Data list from output of a computation. */
public class OutputDataList {
    public final List<Data<?>> inputs;
    public final String name;
    public final Computation computation;
    public final List<TypeInformation<?>> outTypes;

    public OutputDataList(
            List<Data<?>> inputs,
            String name,
            Computation computation,
            List<TypeInformation<?>> outTypes) {
        this.name = name;
        this.inputs = inputs;
        this.computation = computation;
        this.outTypes = outTypes;
    }

    public <R> Data<R> get(int index) {
        return new OutputData<>(this, index);
    }
}
