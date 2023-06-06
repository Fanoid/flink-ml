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
import org.apache.flink.ml.common.computation.purefunc.FlinkExecutor;
import org.apache.flink.ml.common.computation.purefunc.FlinkIterationExecutor;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.Collections;
import java.util.List;

/** Computation wrapped from PureFunc. */
public class PureFuncComputation implements Computation {
    private final PureFunc<?> func;
    private final TypeInformation<?> outputType;

    public PureFuncComputation(PureFunc<?> func, TypeInformation<?> outputType) {
        this.func = func;
        this.outputType = outputType;
    }

    @Override
    public int getNumInputs() {
        return func.getNumInputs();
    }

    @Override
    public List<TypeInformation<?>> getOutputTypes() {
        return Collections.singletonList(outputType);
    }

    @Override
    public int getNumOutputs() {
        return 1;
    }

    public PureFunc<?> getFunc() {
        return func;
    }

    @Override
    public List<Iterable<?>> execute(Iterable<?>... inputs) {
        return func.execute(inputs);
    }

    @Override
    public List<DataStream<?>> executeOnFlink(DataStream<?>... inputs) {
        DataStream<?> input = inputs[0];
        if (func instanceof MapPureFunc) {
            //noinspection unchecked,rawtypes
            DataStream<?> output = FlinkExecutor.execute(input, (MapPureFunc) func, outputType);
            return Collections.singletonList(output);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Not supported for %s yet.", func.getClass().getSimpleName()));
        }
    }

    @Override
    public List<DataStream<?>> executeInIterations(DataStream<?>... inputs) {
        DataStream<?> input = inputs[0];
        if (func instanceof MapPureFunc) {
            //noinspection unchecked,rawtypes
            DataStream<?> output =
                    FlinkIterationExecutor.execute(input, (MapPureFunc) func, outputType);
            return Collections.singletonList(output);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Not supported for %s yet.", func.getClass().getSimpleName()));
        }
    }
}
