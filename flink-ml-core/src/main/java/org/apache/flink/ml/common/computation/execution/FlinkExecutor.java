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
import org.apache.flink.ml.common.computation.builder.PartitionStrategy;
import org.apache.flink.ml.common.computation.builder.PartitionedData;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.computation.Computation;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapPartitionWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecuteMapWithDataPureFuncOperator;
import org.apache.flink.ml.common.computation.execution.FlinkExecutorUtils.ExecutorMapPartitionPureFuncOperator;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.PureFunc;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
            DataStream in, MapPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMap", outType, new ExecuteMapPureFuncOperator(func, in.getParallelism()));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapWithData(
            DataStream in,
            DataStream data,
            MapWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapWithData",
                        outType,
                        new ExecuteMapWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }

    @Override
    public <IN, OUT> DataStream executeMapPartition(
            DataStream in, MapPartitionPureFunc<IN, OUT> func, TypeInformation<OUT> outType) {
        return in.transform(
                "ExecuteMapPartition",
                outType,
                new ExecutorMapPartitionPureFuncOperator<>(func, in.getParallelism()));
    }

    @Override
    public <IN, DATA, OUT> DataStream executeMapPartitionWithData(
            DataStream in,
            DataStream data,
            MapPartitionWithDataPureFunc<IN, DATA, OUT> func,
            TypeInformation<OUT> outType) {
        return in.connect(data)
                .transform(
                        "ExecuteMapPartitionWithData",
                        outType,
                        new ExecuteMapPartitionWithDataPureFuncOperator<>(
                                func, in.getParallelism(), in.getType(), data.getType()));
    }

    @Override
    public <OUT> DataStream<OUT> executeOtherPureFunc(
            List<DataStream> inputs, PureFunc<OUT> func, TypeInformation<OUT> outType) {
        return (DataStream<OUT>) func.executeOnFlink((List<DataStream<?>>) (List) inputs).get(0);
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
        List<DataStream<?>> outputsRecords = computation.executeOnFlink(inputsRecords);
        outputDataListRecordsMap.put(outputDataList, outputsRecords);
        return outputsRecords;
    }

    private DataStream<?> calcDataRecords(
            Data<?> data,
            Map<Data<?>, DataStream<?>> dataRecordsMap,
            Map<OutputDataList, List<DataStream<?>>> outputDataListRecordsMap)
            throws Exception {
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
            PartitionStrategy strategy = partitionedData.getPartitionStrategy();

            DataStream<?> upstreamRecords = dataRecordsMap.get(data.getUpstreams().get(0));
            DataStream<?> records;
            switch (strategy) {
                case ALL:
                    records = upstreamRecords.map(d -> d).setParallelism(1);
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
        } else {
            Preconditions.checkState(1 == upstreams.size());
            dataRecordsMap.put(data, dataRecordsMap.get(data.getUpstreams().get(0)));
        }
        return dataRecordsMap.get(data);
    }

    @Override
    public List<DataStream> execute(CompositeComputation computation, List<DataStream> inputs)
            throws Exception {
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
    public List<DataStream> execute(Computation computation, List<DataStream> inputs) {
        return null;
    }
}
