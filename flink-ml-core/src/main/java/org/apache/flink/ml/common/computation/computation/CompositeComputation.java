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
import org.apache.flink.ml.common.computation.execution.FlinkExecutor;
import org.apache.flink.ml.common.computation.execution.FlinkIterationExecutor;
import org.apache.flink.ml.common.computation.execution.IterableExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Composite computation. */
public class CompositeComputation implements Computation {
    private final List<Data<?>> starts;
    private final List<Data<?>> ends;
    private final List<TypeInformation<?>> outTypes;

    public CompositeComputation(List<Data<?>> starts, List<Data<?>> ends) {
        this.starts = starts;
        this.ends = ends;
        outTypes = ends.stream().map(Data::getType).collect(Collectors.toList());
        if (!checkValidity()) {
            throw new IllegalArgumentException(
                    "All data in ends must depend only on data in starts.");
        }
    }

    private static boolean checkUpStreams(Data<?> data, Set<Data<?>> validSet) {
        if (validSet.contains(data)) {
            return true;
        }
        boolean valid = data.getUpstreams().stream().allMatch(d -> checkUpStreams(d, validSet));
        if (valid) {
            validSet.add(data);
        }
        return valid;
    }

    public List<Data<?>> getStarts() {
        return starts;
    }

    public List<Data<?>> getEnds() {
        return ends;
    }

    @Override
    public int getNumInputs() {
        return starts.size();
    }

    @Override
    public List<TypeInformation<?>> getOutTypes() {
        return outTypes;
    }

    private boolean checkValidity() {
        Set<Data<?>> validSet = new HashSet<>(starts);
        return ends.stream().allMatch(d -> checkUpStreams(d, validSet));
    }

    @Override
    public List<Iterable<?>> execute(List<Iterable<?>> inputs) throws Exception {
        Preconditions.checkArgument(inputs.size() == starts.size());
        return IterableExecutor.getInstance().execute(this, inputs);
    }

    @Override
    public List<DataStream<?>> executeOnFlink(List<DataStream<?>> inputs) throws Exception {
        //noinspection unchecked,rawtypes
        return (List<DataStream<?>>)
                (List) FlinkExecutor.getInstance().execute(this, (List<DataStream>) (List) inputs);
    }

    @Override
    public List<DataStream<?>> executeInIterations(List<DataStream<?>> inputs) throws Exception {
        //noinspection unchecked,rawtypes
        return (List<DataStream<?>>)
                (List)
                        FlinkIterationExecutor.getInstance()
                                .execute(this, (List<DataStream>) (List) inputs);
    }
}
