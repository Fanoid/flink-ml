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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("rawtypes")
class StateHandler implements StreamOperatorStateHandler.CheckpointedStreamOperator {

    private final List<StateDesc<?, ?>> stateDescs;

    @SuppressWarnings("rawtypes")
    private final List<Tuple2<StateDesc, State>> states = new ArrayList<>();

    StateHandler(List<StateDesc<?, ?>> stateDescs) {
        this.stateDescs = stateDescs;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        for (StateDesc stateDesc : stateDescs) {
            if (stateDesc.desc instanceof ListStateDescriptor) {
                ListState<?> state =
                        context.getOperatorStateStore()
                                .getListState((ListStateDescriptor<?>) stateDesc.desc);
                states.add(Tuple2.of(stateDesc, state));
                //noinspection unchecked
                stateDesc.initializeFn.accept(state);
            } else {
                throw new UnsupportedOperationException("Not support non-ListStateDescriptor yet.");
            }
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        for (Tuple2<StateDesc, State> tuple : states) {
            //noinspection unchecked
            tuple.f0.snapshotFn.accept(tuple.f1);
        }
    }
}
