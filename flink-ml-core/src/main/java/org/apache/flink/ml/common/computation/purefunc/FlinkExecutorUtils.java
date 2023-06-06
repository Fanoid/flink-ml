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

import org.apache.flink.iteration.IterationListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;

class FlinkExecutorUtils {
    static class ExecutorMapPartitionPureFuncOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput, IterationListener<OUT> {
        private final MapPartitionPureFunc<IN, OUT> func;
        private final int inputParallelism;

        private transient InputIterator<IN> inputIterator;

        private transient Collector<OUT> collector;

        private transient StateHandler stateHandler;

        private transient Future<?> future;

        private transient PureFuncContextImpl pureFuncContext;

        public ExecutorMapPartitionPureFuncOperator(
                MapPartitionPureFunc<IN, OUT> func, int inputParallelism) {
            this.func = func;
            this.inputParallelism = inputParallelism;
        }

        @Override
        public void open() throws Exception {
            super.open();
            pureFuncContext =
                    new PureFuncContextImpl(
                            getRuntimeContext().getNumberOfParallelSubtasks(),
                            getRuntimeContext().getIndexOfThisSubtask(),
                            inputParallelism);
            if (func instanceof RichMapPartitionPureFunc) {
                ((RichMapPartitionPureFunc<IN, OUT>) func).setContext(pureFuncContext);
                ((RichMapPartitionPureFunc<IN, OUT>) func).open();
            }
            inputIterator = new InputIterator<>();

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
            future = executorService.submit(() -> func.map(() -> inputIterator, collector));
        }

        @Override
        public void processElement(StreamRecord<IN> element) {
            inputIterator.add(element.getValue());
        }

        @Override
        public void endInput() throws Exception {
            inputIterator.end();
            future.get();
            if (func instanceof RichMapPartitionPureFunc) {
                ((RichMapPartitionPureFunc<IN, OUT>) func).close(collector);
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            if (func instanceof RichPureFunc) {
                stateHandler =
                        new StateHandler(
                                ((RichMapPartitionPureFunc<IN, OUT>) func).getStateDescs());
                stateHandler.initializeState(context);
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            if (func instanceof RichMapPartitionPureFunc) {
                stateHandler.snapshotState(context);
            }
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            pureFuncContext.setIteration(epochWatermark);
            if (func instanceof RichMapPartitionPureFunc) {
                ((RichMapPartitionPureFunc<IN, OUT>) func).close(collector);
                ((RichMapPartitionPureFunc<IN, OUT>) func).open();
            }
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OUT> collector)
                throws Exception {
            if (func instanceof RichMapPartitionPureFunc) {
                ((RichMapPartitionPureFunc<IN, OUT>) func).close(collector);
            }
        }
    }

    /**
     * An iterator which can dynamically add elements until explicitly ends.
     *
     * <p>Note that {@link #add} and {@link #end} must be called in a different thread of {@link
     * #next} and {@link #hasNext}.
     *
     * @param <T> The type of the elements.
     */
    static class InputIterator<T> implements Iterator<T> {

        /**
         * A queue to transfer elements from the caller of {@link #add} to the caller of {@link
         * #next}. This queue is synchronous which means {@link #add} and {@link #next} must be
         * called in pairs.
         */
        private final SynchronousQueue<ValueOrEnd<T>> q;

        // The value to be added which is reused multiple times.
        private final ValueOrEnd<T> toAdd = new ValueOrEnd<>();

        // The next value to return.
        private ValueOrEnd<T> next = null;

        public InputIterator() {
            this.q = new SynchronousQueue<>();
        }

        @Override
        public boolean hasNext() {
            if (null == next) {
                next = getNext();
            }
            return !next.isEnd;
        }

        private ValueOrEnd<T> getNext() {
            try {
                return q.take();
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted when getting the next element.");
            }
        }

        @Override
        public T next() {
            if (next.isEnd) {
                throw new NoSuchElementException();
            }
            T savedValue = next.v;
            next = getNext();
            return savedValue;
        }

        public void add(T v) {
            toAdd.v = v;
            q.add(toAdd);
        }

        public void end() {
            toAdd.isEnd = true;
            q.add(toAdd);
        }

        static class ValueOrEnd<T> {
            public boolean isEnd;
            public T v;
        }
    }

    static class ExecutorMapPureFuncOperator<IN, OUT> extends AbstractStreamOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput, IterationListener<OUT> {

        private final MapPureFunc<IN, OUT> func;
        private final int inputParallelism;

        private transient Collector<OUT> collector;

        private transient StateHandler stateHandler;
        private transient PureFuncContextImpl pureFuncContext;

        public ExecutorMapPureFuncOperator(MapPureFunc<IN, OUT> func, int inputParallelism) {
            this.func = func;
            this.inputParallelism = inputParallelism;
        }

        @Override
        public void open() throws Exception {
            super.open();
            if (func instanceof RichMapPureFunc) {
                pureFuncContext =
                        new PureFuncContextImpl(
                                getRuntimeContext().getNumberOfParallelSubtasks(),
                                getRuntimeContext().getIndexOfThisSubtask(),
                                inputParallelism);
                ((RichMapPureFunc<IN, OUT>) func).setContext(pureFuncContext);
                ((RichMapPureFunc<IN, OUT>) func).open();
            }
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            func.map(element.getValue(), collector);
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            pureFuncContext.setIteration(epochWatermark);
            if (func instanceof RichMapPureFunc) {
                ((RichMapPureFunc<IN, OUT>) func).close(collector);
                ((RichMapPureFunc<IN, OUT>) func).open();
            }
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OUT> collector)
                throws Exception {
            if (func instanceof RichMapPureFunc) {
                ((RichMapPureFunc<IN, OUT>) func).close(collector);
            }
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            if (func instanceof RichMapPureFunc) {
                if (null == stateHandler) {
                    stateHandler =
                            new StateHandler(((RichMapPureFunc<IN, OUT>) func).getStateDescs());
                }
                stateHandler.initializeState(context);
            }
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            stateHandler.snapshotState(context);
        }

        @Override
        public void endInput() throws Exception {
            if (func instanceof RichMapPureFunc) {
                ((RichMapPureFunc<IN, OUT>) func).close(collector);
            }
        }
    }
}
