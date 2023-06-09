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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.ml.common.computation.builder.Data;
import org.apache.flink.ml.common.computation.builder.OutputData;
import org.apache.flink.ml.common.computation.builder.OutputDataList;
import org.apache.flink.ml.common.computation.builder.PartitionedData;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.ml.common.computation.computation.IterationComputation;
import org.apache.flink.ml.common.computation.purefunc.ConsumerCollector;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFuncContextImpl;
import org.apache.flink.ml.common.computation.purefunc.RichPureFunc;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.collections.iterators.IteratorChain;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/** Executor with {@link Iterable} as inputs and outputs. */
@SuppressWarnings({"unchecked"})
public class IterableExecutor implements ComputationExecutor<Iterable<?>> {

    private static final IterableExecutor instance = new IterableExecutor();

    public static IterableExecutor getInstance() {
        return instance;
    }

    @Override
    public <IN, OUT> Iterable<OUT> executeMap(
            Iterable<?> in, MapPureFunc<IN, OUT> func, String name, TypeInformation<OUT> outType) {
        //noinspection unchecked,rawtypes
        return () -> new MapPureFuncIterator(in, func);
    }

    @Override
    public <IN, DATA, OUT> Iterable<OUT> executeMapWithData(
            Iterable<?> in,
            Iterable<?> data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        List<DATA> dataList = IteratorUtils.toList(data.iterator());
        Preconditions.checkState(dataList.size() == 1);
        //noinspection unchecked,rawtypes
        return () -> new MapWithDataPureFuncIterator(in, dataList.get(0), func);
    }

    @Override
    public <IN, OUT> Iterable<OUT> executeMapPartition(
            Iterable<?> in,
            MapPartitionPureFunc<IN, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        //noinspection unchecked,rawtypes
        return () -> new MapPartitionPureFuncIterator(in, func);
    }

    @Override
    public <IN, DATA, OUT> Iterable<OUT> executeMapPartitionWithData(
            Iterable<?> in,
            Iterable<?> data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        List<DATA> dataList = IteratorUtils.toList(data.iterator());
        Preconditions.checkState(dataList.size() == 1);
        //noinspection unchecked,rawtypes
        return () -> new MapPartitionWithDataPureFuncIterator(in, dataList.get(0), func);
    }

    @Override
    public <OUT> Iterable<OUT> executeOtherPureFunc(
            List<Iterable<?>> inputs,
            PureFunc<OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        return (Iterable<OUT>) func.execute(inputs).get(0);
    }

    @Override
    public List<Iterable<?>> execute(Computation computation, List<Iterable<?>> inputs) {
        return null;
    }

    private List<List<Iterable<?>>> calcOutputDataListRecords(
            OutputDataList outputDataList,
            Map<Data<?>, List<Iterable<?>>> dataRecordsMap,
            Map<OutputDataList, List<List<Iterable<?>>>> outputDataListRecordsMap) {
        if (outputDataListRecordsMap.containsKey(outputDataList)) {
            return outputDataListRecordsMap.get(outputDataList);
        }

        List<List<Iterable<?>>> inputsRecords =
                outputDataList.inputs.stream()
                        .map(dataRecordsMap::get)
                        .collect(Collectors.toList());
        List<Iterable<?>> mergedInputRecords =
                inputsRecords.stream()
                        .<Iterable<?>>map(d -> new IterableChain(d))
                        .collect(Collectors.<Iterable<?>>toList());

        Computation computation = outputDataList.computation;
        List<Iterable<?>> partitionedInputsRecords = new ArrayList<>(mergedInputRecords);

        List<List<Iterable<?>>> outputsRecords = new ArrayList<>(computation.getNumOutputs());
        for (int i = 0; i < computation.getNumOutputs(); i += 1) {
            outputsRecords.add(new ArrayList<>());
        }

        // Assume only first input is applied partition-wise, other inputs are applied as a
        // whole.
        // TODO: support other partitioned inputs.
        for (Iterable<?> partitionedFirstInput : inputsRecords.get(0)) {
            partitionedInputsRecords.set(0, partitionedFirstInput);
            List<Iterable<?>> partitionedOutputsRecords =
                    computation.execute(partitionedInputsRecords);
            for (int i = 0; i < partitionedOutputsRecords.size(); i++) {
                outputsRecords.get(i).add(partitionedOutputsRecords.get(i));
            }
        }

        outputDataListRecordsMap.put(outputDataList, outputsRecords);
        return outputsRecords;
    }

    private List<Iterable<?>> calcDataRecords(
            Data<?> data,
            Map<Data<?>, List<Iterable<?>>> dataRecordsMap,
            Map<OutputDataList, List<List<Iterable<?>>>> outputDataListRecordsMap) {
        if (dataRecordsMap.containsKey(data)) {
            return dataRecordsMap.get(data);
        }

        List<Data<?>> upstreams = data.getUpstreams();
        for (Data<?> upstream : upstreams) {
            calcDataRecords(upstream, dataRecordsMap, outputDataListRecordsMap);
        }

        if (data instanceof OutputData) {
            OutputData<?> outputData = (OutputData<?>) data;
            OutputDataList outputDataList = outputData.dataList;
            List<List<Iterable<?>>> outputDataListRecords =
                    calcOutputDataListRecords(
                            outputDataList, dataRecordsMap, outputDataListRecordsMap);
            List<Iterable<?>> outputRecords = outputDataListRecords.get(outputData.index);
            dataRecordsMap.put(data, outputRecords);
        } else if (data instanceof PartitionedData) {
            PartitionedData<?> partitionedData = (PartitionedData<?>) data;
            PartitionedData.PartitionStrategy strategy = partitionedData.getPartitionStrategy();
            IterableChain<?> mergedInputIterable =
                    new IterableChain(dataRecordsMap.get(data.getUpstreams().get(0)));
            if (strategy.equals(PartitionedData.PartitionStrategy.GROUP_BY_KEY)) {
                KeySelector keySelector = partitionedData.getKeySelector();
                Map<Object, List> partitions = new HashMap<>();
                for (Object v : mergedInputIterable) {
                    Object key;
                    try {
                        key = keySelector.getKey(v);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    if (!partitions.containsKey(key)) {
                        partitions.put(key, new ArrayList<>());
                    }
                    partitions.get(key).add(v);
                }
                List<Iterable<?>> partitionedRecords =
                        partitions.values().stream()
                                .map(d -> (Iterable<?>) d)
                                .collect(Collectors.toList());
                dataRecordsMap.put(data, partitionedRecords);
            } else {
                dataRecordsMap.put(data, Collections.singletonList(mergedInputIterable));
            }
            return null;
        } else {
            Preconditions.checkState(1 == upstreams.size());
            dataRecordsMap.put(data, dataRecordsMap.get(data.getUpstreams().get(0)));
        }
        return dataRecordsMap.get(data);
    }

    @Override
    public List<Iterable<?>> execute(CompositeComputation computation, List<Iterable<?>> inputs) {
        Preconditions.checkArgument(computation.getNumInputs() == inputs.size());

        // List<Iterable<?>> is used to represent records of a Data, as there could be partitioned
        // data.
        Map<Data<?>, List<Iterable<?>>> dataRecordsMap = new HashMap<>();
        Map<OutputDataList, List<List<Iterable<?>>>> outputDataListRecordsMap = new HashMap<>();

        List<Data<?>> starts = computation.getStarts();
        for (int i = 0; i < computation.getNumInputs(); i += 1) {
            dataRecordsMap.put(starts.get(i), Collections.singletonList(inputs.get(i)));
        }

        for (Data<?> end : computation.getEnds()) {
            calcDataRecords(end, dataRecordsMap, outputDataListRecordsMap);
        }

        return computation.getEnds().stream()
                .map(dataRecordsMap::get)
                .<Iterable<?>>map(d -> new IterableChain(d))
                .collect(Collectors.<Iterable<?>>toList());
    }

    @Override
    public List<Iterable<?>> execute(IterationComputation computation, List<Iterable<?>> inputs) {
        throw new UnsupportedOperationException();
    }

    private static class IterableChain<T> implements Iterable<T> {

        private final List<Iterable<T>> iterables;

        private IterableChain(List<Iterable<T>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<T> iterator() {
            return new IteratorChain(
                    iterables.stream().map(Iterable::iterator).toArray(Iterator[]::new));
        }
    }

    static class MapPureFuncIterator<OUT, IN> implements Iterator<OUT> {
        private final Iterator<IN> iter;
        private final MapPureFunc<IN, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;
        private boolean endInput;

        public MapPureFuncIterator(Iterable<IN> in, MapPureFunc<IN, OUT> fn) {
            this.iter = in.iterator();
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
            if (fn instanceof RichPureFunc) {
                try {
                    ((RichPureFunc<?>) fn).setContext(new PureFuncContextImpl(1, 0, 1));
                    ((RichPureFunc<?>) fn).open();
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call open failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            endInput = false;
        }

        @Override
        public boolean hasNext() {
            while (output.isEmpty() && iter.hasNext()) {
                try {
                    fn.map(iter.next(), collector);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call map failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            if (!endInput && !iter.hasNext()) {
                endInput = true;
                if (fn instanceof RichPureFunc) {
                    try {
                        ((RichPureFunc<OUT>) fn).close(collector);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Call close failed in %s", fn.getClass().getSimpleName()),
                                e);
                    }
                }
            }
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.remove();
        }
    }

    static class MapWithDataPureFuncIterator<OUT, IN, DATA> implements Iterator<OUT> {
        private final Iterator<IN> iter;
        private final DATA data;
        private final MapWithDataPureFunc<IN, DATA, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;
        private boolean endInput;

        public MapWithDataPureFuncIterator(
                Iterable<IN> in, DATA data, MapWithDataPureFunc<IN, DATA, OUT> fn) {
            this.iter = in.iterator();
            this.data = data;
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
            if (fn instanceof RichPureFunc) {
                try {
                    ((RichPureFunc<?>) fn).setContext(new PureFuncContextImpl(1, 0, 1));
                    ((RichPureFunc<?>) fn).open();
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call open failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            endInput = false;
        }

        @Override
        public boolean hasNext() {
            while (output.isEmpty() && iter.hasNext()) {
                try {
                    fn.map(iter.next(), data, collector);
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call map failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            if (!endInput && !iter.hasNext()) {
                endInput = true;
                if (fn instanceof RichPureFunc) {
                    try {
                        ((RichPureFunc<OUT>) fn).close(collector);
                    } catch (Exception e) {
                        throw new RuntimeException(
                                String.format(
                                        "Call close failed in %s", fn.getClass().getSimpleName()),
                                e);
                    }
                }
            }
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.remove();
        }
    }

    static class MapPartitionPureFuncIterator<OUT, IN> implements Iterator<OUT> {
        private final Iterable<IN> in;
        private final MapPartitionPureFunc<IN, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;
        private boolean started;

        public MapPartitionPureFuncIterator(Iterable<IN> in, MapPartitionPureFunc<IN, OUT> fn) {
            this.in = in;
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
            if (fn instanceof RichPureFunc) {
                try {
                    ((RichPureFunc<?>) fn).setContext(new PureFuncContextImpl(1, 0, 1));
                    ((RichPureFunc<?>) fn).open();
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call open failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            started = false;
        }

        @Override
        public boolean hasNext() {
            if (!started) {
                started = true;
                fn.map(in, collector);
            }
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.poll();
        }
    }

    static class MapPartitionWithDataPureFuncIterator<OUT, IN, DATA> implements Iterator<OUT> {
        private final Iterable<IN> in;
        private final DATA data;
        private final MapPartitionWithDataPureFunc<IN, DATA, OUT> fn;
        private final Collector<OUT> collector;
        private final Queue<OUT> output;
        private boolean started;

        public MapPartitionWithDataPureFuncIterator(
                Iterable<IN> in, DATA data, MapPartitionWithDataPureFunc<IN, DATA, OUT> fn) {
            this.in = in;
            this.data = data;
            this.fn = fn;
            output = new ArrayDeque<>();
            collector = new ConsumerCollector<>(output::add);
            if (fn instanceof RichPureFunc) {
                try {
                    ((RichPureFunc<?>) fn).setContext(new PureFuncContextImpl(1, 0, 1));
                    ((RichPureFunc<?>) fn).open();
                } catch (Exception e) {
                    throw new RuntimeException(
                            String.format("Call open failed in %s", fn.getClass().getSimpleName()),
                            e);
                }
            }
            started = false;
        }

        @Override
        public boolean hasNext() {
            if (!started) {
                started = true;
                // TODO: Improve this with SynchronousQueue.
                fn.map(in, data, collector);
            }
            return !output.isEmpty();
        }

        @Override
        public OUT next() {
            return output.poll();
        }
    }
}
