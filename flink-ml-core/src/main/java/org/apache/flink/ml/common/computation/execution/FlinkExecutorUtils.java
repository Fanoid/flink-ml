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

package org.apache.flink.ml.common.computation.execution;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.iteration.IterationListener;
import org.apache.flink.iteration.datacache.nonkeyed.ListStateWithCache;
import org.apache.flink.ml.common.computation.purefunc.ConsumerCollector;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFuncContextImpl;
import org.apache.flink.ml.common.computation.purefunc.RichMapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichMapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichPureFunc;
import org.apache.flink.ml.common.computation.purefunc.StateDesc;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class FlinkExecutorUtils {

    static class ExecutorMapPartitionPureFuncOperator<IN, OUT>
            extends AbstractExecutePureFuncOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
        private final MapPartitionPureFunc<IN, OUT> func;

        private transient InputIterator<IN> inputIterator;
        private transient Collector<OUT> collector;

        private transient ExecutorService executorService;
        private transient Future<?> future;

        public ExecutorMapPartitionPureFuncOperator(
                MapPartitionPureFunc<IN, OUT> func, int inputParallelism, boolean inIterations) {
            super(func, inputParallelism, inIterations);
            this.func = func;
        }

        @Override
        public void open() throws Exception {
            super.open();
            BasicThreadFactory factory =
                    new BasicThreadFactory.Builder()
                            .namingPattern(getOperatorName() + "-input-thread-%d")
                            .build();
            executorService = Executors.newSingleThreadExecutor(factory);
            inputIterator = null;
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            if (null == inputIterator) {
                inputIterator = new InputIterator<>();
                collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
                future = executorService.submit(() -> func.map(() -> inputIterator, collector));
            }
            inputIterator.add(element.getValue(), future);
        }

        @Override
        public void endInput() throws Exception {
            if (null != inputIterator) {
                inputIterator.end(future);
                future.get();
            }
            executorService.shutdown();
            Preconditions.checkState(executorService.isShutdown());
            if (func instanceof RichMapPartitionPureFunc) {
                ((RichMapPartitionPureFunc<IN, OUT>) func).close(collector);
            }
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            if (null != inputIterator) {
                inputIterator.end(future);
                future.get();
                inputIterator = null;
            }
            super.onEpochWatermarkIncremented(epochWatermark, context, collector);
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OUT> collector) {
            executorService.shutdown();
            Preconditions.checkState(executorService.isShutdown());
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
            Preconditions.checkNotNull(savedValue);
            next = getNext();
            return savedValue;
        }

        public void add(T v, Future<?> future) throws Exception {
            while (!q.offer(ValueOrEnd.of(v), 1, TimeUnit.SECONDS)) {
                try {
                    future.get(0, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                }
                Preconditions.checkState(!future.isDone());
            }
        }

        public void end(Future<?> future) throws Exception {
            while (!q.offer(ValueOrEnd.end(), 1, TimeUnit.SECONDS)) {
                try {
                    future.get(0, TimeUnit.MILLISECONDS);
                } catch (TimeoutException ignored) {
                }
                Preconditions.checkState(!future.isDone());
            }
        }

        static class ValueOrEnd<T> {
            public final boolean isEnd;
            public final T v;

            public ValueOrEnd(T v, boolean isEnd) {
                this.isEnd = isEnd;
                this.v = v;
            }

            public static <T> ValueOrEnd<T> of(T v) {
                return new ValueOrEnd<>(v, false);
            }

            public static <T> ValueOrEnd<T> end() {
                return new ValueOrEnd<>(null, true);
            }
        }
    }

    abstract static class AbstractExecutePureFuncOperator<OUT> extends AbstractStreamOperator<OUT>
            implements IterationListener<OUT> {
        protected final PureFunc<OUT> pureFunc;
        protected final int inputParallelism;
        protected final boolean inIterations;

        protected transient PureFuncContextImpl pureFuncContext;
        protected transient StateHandler stateHandler;

        public AbstractExecutePureFuncOperator(
                PureFunc<OUT> pureFunc, int inputParallelism, boolean inIterations) {
            this.pureFunc = pureFunc;
            this.inputParallelism = inputParallelism;
            this.inIterations = inIterations;
        }

        @Override
        public void open() throws Exception {
            super.open();
            pureFuncContext =
                    new PureFuncContextImpl(
                            getRuntimeContext().getNumberOfParallelSubtasks(),
                            getRuntimeContext().getIndexOfThisSubtask(),
                            inputParallelism);
            if (pureFunc instanceof RichPureFunc) {
                // Different from Flink, we set round ID at the start of every round.
                if (inIterations) {
                    pureFuncContext.setIteration(0);
                }
                ((RichPureFunc<OUT>) pureFunc).setContext(pureFuncContext);
                ((RichPureFunc<OUT>) pureFunc).open();
            }
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            if (pureFunc instanceof RichPureFunc) {
                ((RichPureFunc<OUT>) pureFunc).close(collector);
            }
            pureFuncContext.setIteration(epochWatermark);
            if (pureFunc instanceof RichPureFunc) {
                ((RichPureFunc<OUT>) pureFunc).open();
            }
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OUT> collector)
                throws Exception {}

        /** Returns additional states from operators. */
        protected List<StateDesc<?, ?>> addtiionalStateDescs() {
            return Collections.emptyList();
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            if (null == stateHandler) {
                List<StateDesc<?, ?>> stateDescs = new ArrayList<>();
                if (pureFunc instanceof RichMapPureFunc) {
                    stateDescs.addAll(((RichMapPureFunc<?, ?>) pureFunc).getStateDescs());
                }
                stateDescs.addAll(addtiionalStateDescs());
                stateHandler = new StateHandler(stateDescs);
            }
            stateHandler.initializeState(context);
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            stateHandler.snapshotState(context);
        }

        /** The method to be called when the input stream is ended. */
        protected void endOfRecords(Collector<OUT> collector) throws Exception {
            if (!inIterations) {
                if (pureFunc instanceof RichPureFunc) {
                    ((RichPureFunc<OUT>) pureFunc).close(collector);
                }
            }
        }
    }

    static class ExecuteMapPureFuncOperator<IN, OUT> extends AbstractExecutePureFuncOperator<OUT>
            implements OneInputStreamOperator<IN, OUT>, BoundedOneInput {
        private final MapPureFunc<IN, OUT> func;
        private transient Collector<OUT> collector;

        public ExecuteMapPureFuncOperator(
                MapPureFunc<IN, OUT> func, int inputParallelism, boolean inIterations) {
            super(func, inputParallelism, inIterations);
            this.func = func;
        }

        @Override
        public void open() throws Exception {
            super.open();
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
        }

        @Override
        public void processElement(StreamRecord<IN> element) throws Exception {
            func.map(element.getValue(), collector);
        }

        @Override
        public void endInput() throws Exception {
            endOfRecords(collector);
        }
    }

    static class ExecuteMapWithDataPureFuncOperator<IN, DATA, OUT>
            extends AbstractExecutePureFuncOperator<OUT>
            implements TwoInputStreamOperator<IN, DATA, OUT>, BoundedMultiInput {

        private final MapWithDataPureFunc<IN, DATA, OUT> func;
        private final TypeInformation<IN> inType;
        private final TypeInformation<DATA> dataType;
        private transient DATA data;
        private transient ListStateWithCache<IN> inCache;

        private transient Collector<OUT> collector;

        public ExecuteMapWithDataPureFuncOperator(
                MapWithDataPureFunc<IN, DATA, OUT> func,
                int inputParallelism,
                TypeInformation<IN> inType,
                TypeInformation<DATA> dataType,
                boolean inIterations) {
            super(func, inputParallelism, inIterations);
            this.func = func;
            this.inType = inType;
            this.dataType = dataType;
        }

        @Override
        public void open() throws Exception {
            super.open();
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            data = null;
            super.onEpochWatermarkIncremented(epochWatermark, context, collector);
            inCache.clear();
        }

        @Override
        protected List<StateDesc<?, ?>> addtiionalStateDescs() {
            return Collections.singletonList(
                    StateDesc.singleValueState(
                            "__data", dataType, null, (v) -> data = v, () -> data));
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            inCache =
                    new ListStateWithCache<>(
                            inType.createSerializer(getExecutionConfig()),
                            getContainingTask(),
                            getRuntimeContext(),
                            context,
                            config.getOperatorID());
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            inCache.snapshotState(context);
        }

        @Override
        public void processElement1(StreamRecord<IN> element) throws Exception {
            if (null != data) {
                func.map(element.getValue(), data, collector);
            } else {
                inCache.add(element.getValue());
            }
        }

        @Override
        public void processElement2(StreamRecord<DATA> element) throws Exception {
            Preconditions.checkState(null == data);
            data = element.getValue();
            for (IN value : inCache.get()) {
                func.map(value, data, collector);
            }
            inCache.clear();
        }

        @Override
        public void endInput(int inputId) throws Exception {
            if (1 == inputId) {
                endOfRecords(collector);
            }
        }
    }

    static class ExecuteMapPartitionWithDataPureFuncOperator<IN, DATA, OUT>
            extends AbstractExecutePureFuncOperator<OUT>
            implements TwoInputStreamOperator<IN, DATA, OUT>, BoundedMultiInput {
        private final MapPartitionWithDataPureFunc<IN, DATA, OUT> func;
        private final TypeInformation<IN> inType;
        private final TypeInformation<DATA> dataType;

        private transient DATA data;
        private transient ListStateWithCache<IN> inCache;

        private transient InputIterator<IN> inputIterator;

        private transient Collector<OUT> collector;

        private transient ExecutorService executorService;
        private transient Future<?> future;

        public ExecuteMapPartitionWithDataPureFuncOperator(
                MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
                int inputParallelism,
                TypeInformation<IN> inType,
                TypeInformation<DATA> dataType,
                boolean inIterations) {
            super(func, inputParallelism, inIterations);
            this.func = func;
            this.inType = inType;
            this.dataType = dataType;
        }

        @Override
        public void open() throws Exception {
            super.open();
            BasicThreadFactory factory =
                    new BasicThreadFactory.Builder()
                            .namingPattern(getOperatorName() + "-input-thread-%d")
                            .build();
            executorService = Executors.newSingleThreadExecutor(factory);
        }

        @Override
        protected List<StateDesc<?, ?>> addtiionalStateDescs() {
            return Collections.singletonList(
                    StateDesc.singleValueState(
                            "__data", dataType, null, (v) -> data = v, () -> data));
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            inCache =
                    new ListStateWithCache<>(
                            inType.createSerializer(getExecutionConfig()),
                            getContainingTask(),
                            getRuntimeContext(),
                            context,
                            config.getOperatorID());
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            inCache.snapshotState(context);
        }

        protected void startReceiveRecord() {
            inputIterator = new InputIterator<>();
            collector = new ConsumerCollector<>(v -> output.collect(new StreamRecord<>(v)));
            future = executorService.submit(() -> func.map(() -> inputIterator, data, collector));
        }

        protected void finishReceiveRecord() throws Exception {
            if (null != inputIterator && null != collector) {
                inputIterator.end(future);
                future.get();
                inputIterator = null;
            }
        }

        @Override
        public void onEpochWatermarkIncremented(
                int epochWatermark, Context context, Collector<OUT> collector) throws Exception {
            finishReceiveRecord();
            super.onEpochWatermarkIncremented(epochWatermark, context, collector);
            data = null;
            inCache.clear();
        }

        @Override
        public void onIterationTerminated(Context context, Collector<OUT> collector)
                throws Exception {
            executorService.shutdown();
            Preconditions.checkState(executorService.isShutdown());
        }

        @Override
        public void endInput(int inputId) throws Exception {
            if (1 == inputId) {
                if (!inIterations) {
                    finishReceiveRecord();
                    executorService.shutdown();
                    Preconditions.checkState(executorService.isShutdown());
                }
                endOfRecords(collector);
            }
        }

        @Override
        public void processElement1(StreamRecord<IN> element) throws Exception {
            inputIterator.add(element.getValue(), future);
        }

        @Override
        public void processElement2(StreamRecord<DATA> element) throws Exception {
            Preconditions.checkState(null == data);
            // TODO: fixit: data needs to be thread-safe.
            data = element.getValue();
            startReceiveRecord();
            for (IN value : inCache.get()) {
                inputIterator.add(value, future);
            }
            inCache.clear();
        }
    }
}
