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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.iteration.DataStreamList;
import org.apache.flink.iteration.IterationBody;
import org.apache.flink.iteration.IterationBodyResult;
import org.apache.flink.iteration.IterationConfig;
import org.apache.flink.iteration.Iterations;
import org.apache.flink.iteration.ReplayableDataStreamList;
import org.apache.flink.ml.common.computation.builder.Data;
import org.apache.flink.ml.common.computation.builder.OutputData;
import org.apache.flink.ml.common.computation.builder.OutputDataList;
import org.apache.flink.ml.common.computation.builder.PartitionedData;
import org.apache.flink.ml.common.computation.builder.SequentialReadData;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.ml.common.computation.computation.IterationComputation;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapPartitionWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecutorMapPartitionPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** ... */
@SuppressWarnings({"rawtypes", "unchecked"})
public class FlinkExecutor implements ComputationExecutor<DataStream> {

    private static final FlinkExecutor instance = new FlinkExecutor();

    public static FlinkExecutor getInstance() {
        return instance;
    }

    @Override
    public <IN, OUT> DataStream<OUT> executeMap(
            DataStream in, MapPureFunc<IN, OUT> func, String name, TypeInformation<OUT> outType) {
        boolean inIterations = this instanceof FlinkIterationExecutor;
        return in.transform(
                name,
                outType,
                new ExecuteMapPureFuncOperator(func, in.getParallelism(), inIterations));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapWithData(
            DataStream in,
            DataStream data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        boolean inIterations = this instanceof FlinkIterationExecutor;
        return in.connect(data)
                .transform(
                        name,
                        outType,
                        new ExecuteMapWithDataPureFuncOperator<>(
                                func,
                                in.getParallelism(),
                                in.getType(),
                                data.getType(),
                                inIterations));
    }

    @Override
    public <IN, OUT> DataStream executeMapPartition(
            DataStream in,
            MapPartitionPureFunc<IN, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        boolean inIterations = this instanceof FlinkIterationExecutor;
        return in.transform(
                name,
                outType,
                new ExecutorMapPartitionPureFuncOperator<>(
                        func, in.getParallelism(), inIterations));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapPartitionWithData(
            DataStream in,
            DataStream data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            String name,
            TypeInformation<OUT> outType) {
        boolean inIterations = this instanceof FlinkIterationExecutor;
        return in.connect(data)
                .transform(
                        name,
                        outType,
                        new ExecuteMapPartitionWithDataPureFuncOperator<>(
                                func,
                                in.getParallelism(),
                                in.getType(),
                                data.getType(),
                                inIterations));
    }

    private List<DataStream<?>> calcOutputDataListRecords(
            OutputDataList outputDataList,
            Map<Data<?>, DataStream<?>> dataRecordsMap,
            Map<OutputDataList, List<DataStream<?>>> outputDataListRecordsMap) {
        if (outputDataListRecordsMap.containsKey(outputDataList)) {
            return outputDataListRecordsMap.get(outputDataList);
        }
        List<DataStream<?>> inputsRecords =
                outputDataList.inputs.stream()
                        .map(dataRecordsMap::get)
                        .collect(Collectors.toList());
        Computation computation = outputDataList.computation;
        List<DataStream<?>> outputsRecords =
                (List<DataStream<?>>)
                        (List) this.execute(computation, (List<DataStream>) (List) inputsRecords);
        outputDataListRecordsMap.put(outputDataList, outputsRecords);
        return outputsRecords;
    }

    private DataStream<?> calcDataRecords(
            Data<?> data,
            Map<Data<?>, DataStream<?>> dataRecordsMap,
            Map<OutputDataList, List<DataStream<?>>> outputDataListRecordsMap) {
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
            List<DataStream<?>> outputDataListRecords =
                    calcOutputDataListRecords(
                            outputDataList, dataRecordsMap, outputDataListRecordsMap);
            DataStream<?> outputRecords = outputDataListRecords.get(outputData.index);
            dataRecordsMap.put(data, outputRecords);
        } else if (data instanceof PartitionedData) {
            PartitionedData<?> partitionedData = (PartitionedData<?>) data;
            PartitionedData.PartitionStrategy strategy = partitionedData.getPartitionStrategy();

            DataStream<?> upstreamRecords = dataRecordsMap.get(data.getUpstreams().get(0));
            DataStream<?> records;
            switch (strategy) {
                case ALL:
                    records = upstreamRecords.keyBy(value -> 0);
                    break;
                case BROADCAST:
                    records = upstreamRecords.broadcast();
                    break;
                case REBALANCE:
                    records = upstreamRecords.rebalance();
                    break;
                case GROUP_BY_KEY:
                    KeySelector keySelector = partitionedData.getKeySelector();
                    records = upstreamRecords.keyBy(keySelector);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + strategy);
            }
            dataRecordsMap.put(data, records);
        } else if (data instanceof SequentialReadData) {
            // TODO: Cache data to ListStateWithCache
        } else {
            Preconditions.checkState(1 == upstreams.size());
            dataRecordsMap.put(data, dataRecordsMap.get(data.getUpstreams().get(0)));
        }
        return dataRecordsMap.get(data);
    }

    @Override
    public List<DataStream> execute(CompositeComputation computation, List<DataStream> inputs) {
        Preconditions.checkArgument(computation.getNumInputs() == inputs.size());

        // DataStream<?> is used to represent records of a Data, as there could be partitioned
        // data.
        Map<Data<?>, DataStream<?>> dataRecordsMap = new HashMap<>();
        Map<OutputDataList, List<DataStream<?>>> outputDataListRecordsMap = new HashMap<>();

        List<Data<?>> starts = computation.getStarts();
        for (int i = 0; i < computation.getNumInputs(); i += 1) {
            dataRecordsMap.put(starts.get(i), inputs.get(i));
        }

        for (Data<?> end : computation.getEnds()) {
            calcDataRecords(end, dataRecordsMap, outputDataListRecordsMap);
        }

        return computation.getEnds().stream().map(dataRecordsMap::get).collect(Collectors.toList());
    }

    @Override
    public List<DataStream> execute(IterationComputation computation, List<DataStream> inputs) {
        Preconditions.checkArgument(computation.getNumInputs() == inputs.size());

        Map<Integer, Integer> feedbackMap = computation.feedbackMap;
        Set<Integer> replayableInputs = computation.replayInputs;

        Map<Integer, Integer> reversedFeedbackMap =
                feedbackMap.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        // 0: variable stream, 1: replayable data stream, 2: non-replayable data stream.
        int[] inputTypes = new int[inputs.size()];
        int replayableCounter = 0;
        for (int i = 0; i < inputs.size(); i += 1) {
            if (reversedFeedbackMap.containsKey(i)) {
                inputTypes[i] = 0;
                Preconditions.checkNotNull(
                        !replayableInputs.contains(i),
                        "A input cannot be replayable and the feedback target at the same time.");
            } else {
                if (replayableInputs.contains(i)) {
                    inputTypes[i] = 1;
                    replayableCounter += 1;
                } else {
                    inputTypes[i] = 2;
                }
            }
        }

        List<DataStream<?>> initVar = new ArrayList<>();
        List<DataStream<?>> replayable = new ArrayList<>();
        List<DataStream<?>> nonReplayable = new ArrayList<>();
        Map<Integer, Integer> inputToVarIndex = new HashMap<>();
        Map<Integer, Integer> inputToDataIndex = new HashMap<>();

        for (int i = 0; i < inputs.size(); i += 1) {
            DataStream<?> stream = inputs.get(i);
            if (inputTypes[i] == 0) {
                inputToVarIndex.put(i, initVar.size());
                initVar.add(stream);
            } else if (inputTypes[i] == 1) {
                inputToDataIndex.put(i, replayable.size());
                replayable.add(stream);
            } else if (inputTypes[i] == 2) {
                inputToDataIndex.put(i, nonReplayable.size() + replayableCounter);
                nonReplayable.add(stream);
            }
        }

        IterationBody body =
                new IterationBody() {
                    @Override
                    public IterationBodyResult process(
                            DataStreamList variableStreams, DataStreamList dataStreams) {

                        final Computation step = computation.step;
                        List<DataStream<?>> stepInputs = new ArrayList<>();
                        for (int i = 0; i < step.getNumInputs(); i += 1) {
                            if (inputToDataIndex.containsKey(i)) {
                                stepInputs.add(dataStreams.get(inputToDataIndex.get(i)));
                            } else {
                                stepInputs.add(variableStreams.get(inputToVarIndex.get(i)));
                            }
                        }
                        List<DataStream<?>> stepOutputs = step.executeInIterations(stepInputs);

                        List<DataStream<?>> feedback = new ArrayList<>();
                        for (int i = 0; i < variableStreams.size(); i += 1) {
                            feedback.add(null);
                        }
                        for (int outIndex : feedbackMap.keySet()) {
                            int varIndex = inputToVarIndex.get(feedbackMap.get(outIndex));
                            feedback.set(varIndex, stepOutputs.get(outIndex));
                        }

                        DataStream<Boolean> isEnd =
                                ((DataStream<Boolean>)
                                                stepOutputs.get(computation.endCriteriaIndex))
                                        .flatMap(
                                                (v, out) -> {
                                                    if (!v) {
                                                        out.collect(v);
                                                    }
                                                },
                                                Types.BOOLEAN);

                        return new IterationBodyResult(
                                DataStreamList.of(feedback.toArray(new DataStream[0])),
                                DataStreamList.of(
                                        computation.outputMapping.stream()
                                                .map(stepOutputs::get)
                                                .toArray(DataStream[]::new)),
                                isEnd);
                    }
                };

        DataStreamList outputs =
                Iterations.iterateBoundedStreamsUntilTermination(
                        new DataStreamList(initVar),
                        ReplayableDataStreamList.replay(replayable.toArray(new DataStream[0]))
                                .andNotReplay(nonReplayable.toArray(new DataStream[0])),
                        IterationConfig.newBuilder()
                                .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
                                .build(),
                        body);
        return (List<DataStream>) (List) outputs.getDataStreams();
    }
}
