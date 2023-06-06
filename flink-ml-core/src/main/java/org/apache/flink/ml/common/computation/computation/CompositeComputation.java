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

package org.apache.flink.ml.common.computation.computation;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.computation.builder.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;

/** Composite computation. */
public class CompositeComputation implements Computation {
    private final List<Data<?>> starts;
    private final List<Data<?>> ends;
    private final List<TypeInformation<?>> outTypes;

    public CompositeComputation(List<Data<?>> starts, List<Data<?>> ends) {
        this.starts = starts;
        this.ends = ends;
        outTypes = ends.stream().map(d -> d.type).collect(Collectors.toList());
    }

    @Override
    public int getNumInputs() {
        return starts.size();
    }

    @Override
    public List<TypeInformation<?>> getOutTypes() {
        return outTypes;
    }

    @Override
    public List<Iterable<?>> execute(Iterable<?>... inputs) {
        Preconditions.checkArgument(inputs.length == starts.size());
        return Computation.super.execute(inputs);
    }

    @Override
    public List<DataStream<?>> executeOnFlink(DataStream<?>... inputs) {
        return Computation.super.executeOnFlink(inputs);
    }

    @Override
    public List<DataStream<?>> executeInIterations(DataStream<?>... inputs) {
        return Computation.super.executeInIterations(inputs);
    }
}
