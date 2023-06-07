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
import org.apache.flink.ml.common.computation.execution.FlinkExecutor;
import org.apache.flink.ml.common.computation.execution.FlinkIterationExecutor;
import org.apache.flink.ml.common.computation.execution.IterableExecutor;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collections;
import java.util.List;

/** Computation wrapped from PureFunc. */
public class PureFuncComputation implements Computation {
    private final PureFunc<?> func;
    private final String name;
    private final TypeInformation<?> outType;

    public PureFuncComputation(PureFunc<?> func, String name, TypeInformation<?> outType) {
        this.func = func;
        this.name = name;
        this.outType = outType;
    }

    @Override
    public int getNumInputs() {
        return func.getNumInputs();
    }

    @Override
    public List<TypeInformation<?>> getOutTypes() {
        return Collections.singletonList(outType);
    }

    @Override
    public int getNumOutputs() {
        return 1;
    }

    public PureFunc<?> getFunc() {
        return func;
    }

    public String getName() {
        return name;
    }

    @Override
    public List<Iterable<?>> execute(List<Iterable<?>> inputs) throws Exception {
        return Collections.singletonList(IterableExecutor.getInstance().execute(this, inputs));
    }

    @Override
    public List<DataStream<?>> executeOnFlink(List<DataStream<?>> inputs) throws Exception {
        //noinspection unchecked,rawtypes
        return Collections.singletonList(
                FlinkExecutor.getInstance().execute(this, (List<DataStream>) (List) inputs));
    }

    @Override
    public List<DataStream<?>> executeInIterations(List<DataStream<?>> inputs) throws Exception {
        //noinspection unchecked,rawtypes
        return Collections.singletonList(
                FlinkIterationExecutor.getInstance()
                        .execute(this, (List<DataStream>) (List) (inputs)));
    }
}
