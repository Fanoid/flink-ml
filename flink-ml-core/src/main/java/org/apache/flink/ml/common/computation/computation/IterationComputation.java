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
import org.apache.flink.iteration.Iterations;
import org.apache.flink.ml.common.computation.execution.FlinkExecutor;
import org.apache.flink.ml.common.computation.execution.FlinkIterationExecutor;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The computation representing iterations.
 *
 * <p>Basically, the convention proposed by {@link Iterations} is followed and simplified to more
 * natural usage:
 *
 * <ol>
 *   <li>A {@link #step} computation is executed in every iteration.
 *   <li>After every iteration, some outputs of {@link #step} are fed back to inputs of {@link
 *       #step} specified with {@link #feedbackMap}. These inputs being fed back are identified as
 *       `variables` (similar with the concepts of variable streams in {@link Iterations}), while
 *       other inputs are identified as `data`.
 *   <li>Before the first iteration, the inputs of {@link IterationComputation} are 1-1
 *       correspondents with the inputs of {@link #step} .
 *   <li>Some outputs of {@link #step} are used as the outputs of {@link IterationComputation}.
 *       There correspondences are specified using {@link #outputMapping}.
 * </ol>
 */
public class IterationComputation implements Computation {

    // The computation for one step in the iterations.
    public final Computation step;

    /** Feeds the outputs of {@link #step} to its inputs. */
    public final Map<Integer, Integer> feedbackMap;

    /** Maps the outputs of {@link IterationComputation} to the outputs of {@link #step}. */
    public final List<Integer> outputMapping;

    // Input indices to be replayed in every iteration
    public final Set<Integer> replayInputs;

    // The output index indicating the end of iterations, whose type must be boolean.
    public final int endCriteriaIndex;

    public IterationComputation(
            Computation step,
            Map<Integer, Integer> feedbackMap,
            List<Integer> outputMapping,
            int endCriteriaIndex,
            Set<Integer> replayableIndices) {
        this.step = step;
        this.feedbackMap = feedbackMap;
        this.outputMapping = outputMapping;
        this.endCriteriaIndex = endCriteriaIndex;
        this.replayInputs = replayableIndices;
    }

    @Override
    public int getNumInputs() {
        return step.getNumInputs();
    }

    @Override
    public int getNumOutputs() {
        return outputMapping.size();
    }

    @Override
    public List<TypeInformation<?>> getOutTypes() {
        List<TypeInformation<?>> stepOutTypes = step.getOutTypes();
        return outputMapping.stream().map(stepOutTypes::get).collect(Collectors.toList());
    }

    @Override
    public List<Iterable<?>> execute(List<Iterable<?>> inputs) {
        return Computation.super.execute(inputs);
    }

    @Override
    public List<DataStream<?>> executeOnFlink(List<DataStream<?>> inputs) {
        //noinspection unchecked,rawtypes
        return (List<DataStream<?>>)
                (List) FlinkExecutor.getInstance().execute(this, (List<DataStream>) (List) inputs);
    }

    @Override
    public List<DataStream<?>> executeInIterations(List<DataStream<?>> inputs) {
        //noinspection unchecked,rawtypes
        return (List<DataStream<?>>)
                (List)
                        FlinkIterationExecutor.getInstance()
                                .execute(this, (List<DataStream>) (List) inputs);
    }
}
