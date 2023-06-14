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
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * A base interface for all rich pure functions.
 *
 * <p>This interface defines methods for the life cycle of the functions, methods to access the
 * context in which the functions are executed, and methods to provide state descriptors.
 *
 * <p>Note that all {@link PureFunc}s are expected to run in iterations and out of iterations, and
 * the life cycle functions, i.e., {@link #open} and {@link #close}, could be called multiple times
 * when called in iterations. Therefore, initialization and cleanup codes should be carefully
 * written.
 */
@Experimental
public interface RichPureFunc<OUT> extends PureFunc<OUT> {

    /**
     * Initialization method for the function. It is called before the actual working methods (i.e.,
     * map) and thus suitable for setup work. For functions that are part of an iteration, this
     * method will be invoked at the beginning of each iteration.
     *
     * <p>To be specific, this method is called at the end of {@link StreamOperator#open} when
     * executed in {@link Computation#executeOnFlink}, or at the end of {@link
     * IterationListener#onEpochWatermarkIncremented} when using {@link
     * Computation#executeInIterations}.
     */
    void open() throws Exception;

    /**
     * Tear-down method for the user code. It is called after the last call to the main working
     * methods (i.e., map ). For functions that are part of an iteration, this method will be
     * invoked after each iteration.
     *
     * <p>To be specific, this method is called at the start of {@link BoundedOneInput#endInput} or
     * {@link BoundedMultiInput#endInput}(only when inputId=1) when executed in {@link
     * Computation#executeOnFlink}, or at the start of {@link
     * IterationListener#onEpochWatermarkIncremented} when using {@link
     * Computation#executeInIterations}.
     */
    void close(Collector<OUT> out) throws Exception;

    /**
     * Gets pure func context.
     *
     * @return An instance of pure func context.
     */
    PureFuncContext getContext();

    void setContext(PureFuncContext context);

    /**
     * Gets state descriptors in this pure func.
     *
     * @return State descriptors.
     */
    List<StateDesc<?, ?>> getStateDescs();
}
