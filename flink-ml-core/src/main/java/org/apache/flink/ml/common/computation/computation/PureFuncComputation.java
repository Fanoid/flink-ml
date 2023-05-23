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

import org.apache.flink.ml.common.computation.purefunc.PureFunc;

/** Computation wrapped from PureFunc. */
public class PureFuncComputation implements Computation {
    private final PureFunc func;
    private final int numInputs;
    private final int numOutputs;

    public PureFuncComputation(PureFunc func) {
        this.func = func;
        // TODO: calculate it from PureFunc
        numInputs = 1;
        numOutputs = 1;
    }

    @Override
    public int getNumInputs() {
        return numInputs;
    }

    @Override
    public int getNumOutputs() {
        return numOutputs;
    }

    public PureFunc getFunc() {
        return func;
    }
}
