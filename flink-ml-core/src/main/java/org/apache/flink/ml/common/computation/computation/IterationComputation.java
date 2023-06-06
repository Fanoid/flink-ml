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

import java.util.List;
import java.util.Map;

/** The computation representing iterations. */
public class IterationComputation implements Computation {

    // THe computation for one iteration, which has n inputs, m outputs
    public final Computation oneIteration;

    // Input indices to be replayed in every iteration
    public final List<Integer> replayInputs;

    // Feedback output indices to input indices
    public final Map<Integer, Integer> feedbackMap;

    // The output index indicating the end of iterations, whose type must be boolean.
    public final int endCriteriaIndex;

    // Output indices
    public final List<Integer> outputs;

    public IterationComputation(
            Computation oneIteration,
            List<Integer> replayInputs,
            Map<Integer, Integer> feedbackMap,
            int endCriteriaIndex,
            List<Integer> outputs) {
        this.oneIteration = oneIteration;
        this.replayInputs = replayInputs;
        this.feedbackMap = feedbackMap;
        this.endCriteriaIndex = endCriteriaIndex;
        this.outputs = outputs;
    }

    @Override
    public int getNumInputs() {
        return oneIteration.getNumInputs();
    }

    @Override
    public int getNumOutputs() {
        return outputs.size();
    }

    @Override
    public List<TypeInformation<?>> getOutTypes() {
        // TODO: fixit.
        return null;
    }
}
