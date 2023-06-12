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

/** Context for pure functions. */
@Experimental
public class PureFuncContextImpl implements PureFuncContext {

    private final int numSubtasks;
    private final int subtaskId;
    private final int inputParallelism;
    private boolean inIterations;
    private int interation;

    public PureFuncContextImpl(int numSubtasks, int subtaskId, int inputParallelism) {
        this.numSubtasks = numSubtasks;
        this.subtaskId = subtaskId;
        this.inputParallelism = inputParallelism;
        this.inIterations = false;
        this.interation = 0;
    }

    @Override
    public int getNumSubtasks() {
        return numSubtasks;
    }

    @Override
    public int getSubtaskId() {
        return subtaskId;
    }

    @Override
    public int getInputParallelism() {
        return inputParallelism;
    }

    @Override
    public int getIteration() {
        if (inIterations) {
            return interation;
        } else {
            throw new IllegalStateException("Cannot get iteration in non-iterations execution.");
        }
    }

    public void setIteration(int iteration) {
        this.inIterations = true;
        this.interation = iteration;
    }

    public boolean isInIterations() {
        return inIterations;
    }
}
