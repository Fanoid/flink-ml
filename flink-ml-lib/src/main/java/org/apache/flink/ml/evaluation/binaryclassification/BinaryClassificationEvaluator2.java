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

package org.apache.flink.ml.evaluation.binaryclassification;

import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.ml.api.AlgoOperator;
import org.apache.flink.ml.common.computation.builder.Data;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.purefunc.MapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.ReducePureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichMapPartitionPureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichMapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.RichMapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.StateDesc;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * An AlgoOperator which calculates the evaluation metrics for binary classification. The input data
 * has columns rawPrediction, label and an optional weight column. The rawPrediction can be of type
 * double (binary 0/1 prediction, or probability of label 1) or of type vector (length-2 vector of
 * raw predictions, scores, or label probabilities). The output may contain different metrics which
 * will be defined by parameter MetricsNames. See {@link BinaryClassificationEvaluatorParams}.
 */
public class BinaryClassificationEvaluator2
        implements AlgoOperator<BinaryClassificationEvaluator2>,
                BinaryClassificationEvaluatorParams<BinaryClassificationEvaluator2> {
    private static final int NUM_SAMPLE_FOR_RANGE_PARTITION = 100;
    private static final Logger LOG = LoggerFactory.getLogger(BinaryClassificationEvaluator2.class);
    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public BinaryClassificationEvaluator2() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    /**
     * @param values Reduce Summary of all workers.
     * @param taskId current taskId.
     * @return [curTrue, curFalse, TotalTrue, TotalFalse]
     */
    static long[] reduceBinarySummary(List<BinarySummary> values, int taskId) {
        List<BinarySummary> list = new ArrayList<>(values);
        list.sort(Comparator.comparingDouble(t -> -t.maxScore));
        long curTrue = 0;
        long curFalse = 0;
        long totalTrue = 0;
        long totalFalse = 0;

        for (BinarySummary statistics : list) {
            if (statistics.taskId == taskId) {
                curFalse = totalFalse;
                curTrue = totalTrue;
            }
            totalTrue += statistics.curPositive;
            totalFalse += statistics.curNegative;
        }
        return new long[] {curTrue, curFalse, totalTrue, totalFalse};
    }

    /**
     * Updates binary summary by one evaluated element.
     *
     * @param statistics Binary summary.
     * @param evalElement evaluated element.
     */
    static void updateBinarySummary(
            BinarySummary statistics, Tuple3<Double, Boolean, Double> evalElement) {
        if (evalElement.f1) {
            statistics.curPositive++;
        } else {
            statistics.curNegative++;
        }
        if (Double.compare(statistics.maxScore, evalElement.f0) < 0) {
            statistics.maxScore = evalElement.f0;
        }
    }

    public static BinaryClassificationEvaluator2 load(StreamTableEnvironment env, String path)
            throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    /**
     * Calculates boundary range for rangePartition.
     *
     * @param evalData Evaluate data.
     * @return Boundary range.
     */
    private static Data<double[]> getBoundaryRange(Data<Tuple3<Double, Boolean, Double>> evalData) {
        Data<double[]> sampleScoreStream =
                evalData.mapPartition(
                        new SampleScoreFunction(),
                        PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);

        return sampleScoreStream
                .all()
                .mapPartition(
                        new CalcBoundaryRangeFunction(),
                        PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<Row> input = tEnv.toDataStream(inputs[0]);

        Data<Row> data = new Data<>(input.getType());
        Data<Tuple3<Double, Boolean, Double>> evalData =
                data.map(
                        new ParseSample(getLabelCol(), getRawPredictionCol(), getWeightCol()),
                        Types.TUPLE(Types.DOUBLE, Types.BOOLEAN, Types.DOUBLE));
        Data<double[]> boundaryRange = getBoundaryRange(evalData);

        Data<Tuple4<Double, Boolean, Double, Integer>> evalDataWithTaskId =
                evalData.map(
                        new AppendTaskIdPureFunc(),
                        boundaryRange,
                        Types.TUPLE(Types.DOUBLE, Types.BOOLEAN, Types.DOUBLE, Types.INT));

        /* Repartition the evaluated data by range. */
        evalDataWithTaskId = evalDataWithTaskId.groupByKey(x -> x.f3);

        Data<Tuple3<Double, Boolean, Double>> sortEvalData =
                evalDataWithTaskId.mapPartition(
                        new MapPartitionPureFunc<
                                Tuple4<Double, Boolean, Double, Integer>,
                                Tuple3<Double, Boolean, Double>>() {
                            @Override
                            public void map(
                                    Iterable<Tuple4<Double, Boolean, Double, Integer>> values,
                                    Collector<Tuple3<Double, Boolean, Double>> out) {
                                List<Tuple3<Double, Boolean, Double>> bufferedData =
                                        new LinkedList<>();
                                for (Tuple4<Double, Boolean, Double, Integer> t4 : values) {
                                    bufferedData.add(Tuple3.of(t4.f0, t4.f1, t4.f2));
                                }
                                bufferedData.sort(Comparator.comparingDouble(o -> -o.f0));
                                for (Tuple3<Double, Boolean, Double> dataPoint : bufferedData) {
                                    out.collect(dataPoint);
                                }
                            }
                        },
                        Types.TUPLE(Types.DOUBLE, Types.BOOLEAN, Types.DOUBLE));

        /* Calculates the summary of local data. */
        Data<BinarySummary> partitionSummaries =
                sortEvalData.map(
                        new PartitionSummaryPureFunc(), TypeInformation.of(BinarySummary.class));
        Data<List<BinarySummary>> partitionSummariesList =
                partitionSummaries.mapPartition(
                        (values, out) -> {
                            List<BinarySummary> l = new ArrayList<>();
                            for (BinarySummary value : values) {
                                l.add(value);
                            }
                            out.collect(l);
                        },
                        Types.LIST(partitionSummaries.type));

        /* Sorts global data. Output Tuple4 : <score, order, isPositive, weight>. */
        Data<Tuple4<Double, Long, Boolean, Double>> dataWithOrders =
                sortEvalData.map(
                        new CalcSampleOrdersPureFunc(),
                        partitionSummariesList,
                        Types.TUPLE(Types.DOUBLE, Types.BOOLEAN, Types.DOUBLE, Types.INT));

        Data<double[]> localAreaUnderROCVariable =
                dataWithOrders.map(
                        "AccumulateMultiScore",
                        new AccumulateMultiScorePureFunc(),
                        PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO);

        Data<double[]> middleAreaUnderROC =
                localAreaUnderROCVariable.reduce(new AucReducePureFunc());

        Data<Double> areaUnderROC =
                middleAreaUnderROC.map(
                        new MapPureFunc<double[], Double>() {
                            @Override
                            public void map(double[] value, Collector<Double> out) {
                                if (value[1] > 0 && value[2] > 0) {
                                    double v =
                                            (value[0] - 1. * value[1] * (value[1] + 1) / 2)
                                                    / (value[1] * value[2]);
                                    out.collect(v);
                                } else {
                                    out.collect(Double.NaN);
                                }
                            }
                        },
                        Types.DOUBLE);

        CompositeComputation aucComputation =
                new CompositeComputation(
                        Collections.singletonList(data), Collections.singletonList(areaUnderROC));

        //        Map<String, DataStream<?>> broadcastMap = new HashMap<>();
        //        broadcastMap.put(partitionSummariesKey, partitionSummaries);
        //        broadcastMap.put(AREA_UNDER_ROC, areaUnderROC);
        //        DataStream<BinaryMetrics> localMetrics =
        //                BroadcastUtils.withBroadcastStream(
        //                        Collections.singletonList(sortEvalData),
        //                        broadcastMap,
        //                        inputList -> {
        //                            DataStream input = inputList.get(0);
        //                            return DataStreamUtils.mapPartition(
        //                                    input, new CalcBinaryMetrics(partitionSummariesKey));
        //                        });
        //
        //        DataStream<Map<String, Double>> metrics =
        //                DataStreamUtils.mapPartition(
        //                        localMetrics, new MergeMetrics(), Types.MAP(Types.STRING,
        // Types.DOUBLE));
        //        metrics.getTransformation().setParallelism(1);
        //
        //        final String[] metricsNames = getMetricsNames();
        //        TypeInformation<?>[] metricTypes = new TypeInformation[metricsNames.length];
        //        Arrays.fill(metricTypes, Types.DOUBLE);
        //        RowTypeInfo outputTypeInfo = new RowTypeInfo(metricTypes, metricsNames);
        //
        //        DataStream<Row> evalResult =
        //                metrics.map(
        //                        (MapFunction<Map<String, Double>, Row>)
        //                                value -> {
        //                                    Row ret = new Row(metricsNames.length);
        //                                    for (int i = 0; i < metricsNames.length; ++i) {
        //                                        ret.setField(i, value.get(metricsNames[i]));
        //                                    }
        //                                    return ret;
        //                                },
        //                        outputTypeInfo);
        //        return new Table[] {tEnv.fromDataStream(evalResult)};
        return new Table[] {tEnv.fromDataStream(aucComputation.executeOnFlink(input).get(0))};
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    private static class AucReducePureFunc implements ReducePureFunc<double[]> {
        @Override
        public double[] reduce(double[] value1, double[] value2) {
            value2[0] += value1[0];
            value2[1] += value1[1];
            value2[2] += value1[2];
            return value2;
        }
    }

    private static class AccumulateMultiScorePureFunc
            extends RichMapPureFunc<Tuple4<Double, Long, Boolean, Double>, double[]> {

        private double[] accValue;
        private double score;

        @Override
        public void map(Tuple4<Double, Long, Boolean, Double> value, Collector<double[]> out) {
            if (accValue == null) {
                accValue = new double[4];
                score = value.f0;
            } else if (score != value.f0) {
                out.collect(
                        new double[] {
                            accValue[0] / accValue[1] * accValue[2], accValue[2], accValue[3]
                        });
                Arrays.fill(accValue, 0.0);
            }
            accValue[0] += value.f1;
            accValue[1] += 1.0;
            if (value.f2) {
                accValue[2] += value.f3;
            } else {
                accValue[3] += value.f3;
            }
        }

        @Override
        public void close(Collector<double[]> out) {
            if (accValue != null) {
                out.collect(
                        new double[] {
                            accValue[0] / accValue[1] * accValue[2], accValue[2], accValue[3]
                        });
            }
        }

        @Override
        public List<StateDesc<?, ?>> getStateDescs() {
            return Arrays.asList(
                    StateDesc.singleValueState(
                            "accValueState",
                            PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
                            null,
                            (v) -> accValue = v,
                            () -> accValue),
                    StateDesc.singleValueState(
                            "scoreState", Types.DOUBLE, 0., (v) -> score = v, () -> score));
        }
    }

    static class PartitionSummaryPureFunc
            extends RichMapPureFunc<Tuple3<Double, Boolean, Double>, BinarySummary> {

        private BinarySummary summary;
        private BinarySummary defaultV;

        @Override
        public void open() throws Exception {
            defaultV = new BinarySummary(getContext().getSubtaskId(), -Double.MAX_VALUE, 0, 0);
            summary = defaultV;
        }

        @Override
        public void map(Tuple3<Double, Boolean, Double> value, Collector<BinarySummary> out) {
            updateBinarySummary(summary, value);
        }

        @Override
        public void close(Collector<BinarySummary> out) throws Exception {
            out.collect(summary);
        }

        @Override
        public List<StateDesc<?, ?>> getStateDescs() {
            return Collections.singletonList(
                    StateDesc.singleValueState(
                            "summaryState",
                            TypeInformation.of(BinarySummary.class),
                            defaultV,
                            (v) -> summary = v,
                            () -> summary));
        }
    }

    /**
     * For each sample, calculates its score order among all samples. The sample with minimum score
     * has order 1, while the sample with maximum score has order samples.
     *
     * <p>Input is a dataset of tuple (score, is real positive, weight), output is a dataset of
     * tuple (score, order, is real positive, weight).
     */
    private static class CalcSampleOrdersPureFunc
            extends RichMapWithDataPureFunc<
                    Tuple3<Double, Boolean, Double>,
                    List<BinarySummary>,
                    Tuple4<Double, Long, Boolean, Double>> {

        private long[] countValues;
        private long startIndex;
        private long total;

        @Override
        public void map(
                Tuple3<Double, Boolean, Double> value,
                List<BinarySummary> statistics,
                Collector<Tuple4<Double, Long, Boolean, Double>> out) {
            if (null == countValues) {
                countValues = reduceBinarySummary(statistics, getContext().getSubtaskId());
                startIndex = countValues[1] + countValues[0] + 1;
                total = countValues[2] + countValues[3];
            }
            out.collect(Tuple4.of(value.f0, total - startIndex + 1, value.f1, value.f2));
            startIndex++;
        }

        @Override
        public List<StateDesc<?, ?>> getStateDescs() {
            return Collections.emptyList();
        }
    }

    static class AppendTaskIdPureFunc
            implements MapWithDataPureFunc<
                    Tuple3<Double, Boolean, Double>,
                    double[],
                    Tuple4<Double, Boolean, Double, Integer>> {

        @Override
        public void map(
                Tuple3<Double, Boolean, Double> value,
                double[] boundaryRange,
                Collector<Tuple4<Double, Boolean, Double, Integer>> out) {
            for (int i = boundaryRange.length - 1; i > 0; --i) {
                if (value.f0 > boundaryRange[i]) {
                    out.collect(Tuple4.of(value.f0, value.f1, value.f2, i));
                }
            }
            out.collect(Tuple4.of(value.f0, value.f1, value.f2, 0));
        }
    }

    static class SampleScoreFunction
            implements MapPartitionPureFunc<Tuple3<Double, Boolean, Double>, double[]> {
        @Override
        public void map(
                Iterable<Tuple3<Double, Boolean, Double>> dataPoints, Collector<double[]> out) {
            List<Double> bufferedDataPoints = new ArrayList<>();
            for (Tuple3<Double, Boolean, Double> dataPoint : dataPoints) {
                bufferedDataPoints.add(dataPoint.f0);
            }
            if (bufferedDataPoints.size() == 0) {
                return;
            }
            double[] sampleScores = new double[NUM_SAMPLE_FOR_RANGE_PARTITION];
            Arrays.fill(sampleScores, Double.MAX_VALUE);
            Random rand = new Random();
            int sampleNum = bufferedDataPoints.size();
            if (sampleNum > 0) {
                for (int i = 0; i < NUM_SAMPLE_FOR_RANGE_PARTITION; ++i) {
                    sampleScores[i] = bufferedDataPoints.get(rand.nextInt(sampleNum));
                }
            }
            out.collect(sampleScores);
        }
    }

    static class CalcBoundaryRangeFunction extends RichMapPartitionPureFunc<double[], double[]> {

        @Override
        public void map(Iterable<double[]> dataPoints, Collector<double[]> out) {
            int parallelism = getContext().getInputParallelism();
            double[] allSampleScore = new double[parallelism * NUM_SAMPLE_FOR_RANGE_PARTITION];
            int cnt = 0;
            for (double[] dataPoint : dataPoints) {
                System.arraycopy(
                        dataPoint,
                        0,
                        allSampleScore,
                        cnt * NUM_SAMPLE_FOR_RANGE_PARTITION,
                        NUM_SAMPLE_FOR_RANGE_PARTITION);
                cnt++;
            }
            Arrays.sort(allSampleScore);
            double[] boundaryRange = new double[parallelism];
            for (int i = 0; i < parallelism; ++i) {
                boundaryRange[i] = allSampleScore[i * NUM_SAMPLE_FOR_RANGE_PARTITION];
            }
            out.collect(boundaryRange);
        }

        @Override
        public List<StateDesc<?, ?>> getStateDescs() {
            return Collections.emptyList();
        }
    }

    static class ParseSample implements MapPureFunc<Row, Tuple3<Double, Boolean, Double>> {
        private final String labelCol;
        private final String rawPredictionCol;
        private final String weightCol;

        public ParseSample(String labelCol, String rawPredictionCol, String weightCol) {
            this.labelCol = labelCol;
            this.rawPredictionCol = rawPredictionCol;
            this.weightCol = weightCol;
        }

        @Override
        public void map(Row value, Collector<Tuple3<Double, Boolean, Double>> out) {
            double label = ((Number) value.getFieldAs(labelCol)).doubleValue();
            Object probOrigin = value.getField(rawPredictionCol);
            double prob =
                    probOrigin instanceof Vector
                            ? ((Vector) probOrigin).get(1)
                            : ((Number) probOrigin).doubleValue();
            double weight =
                    weightCol == null ? 1.0 : ((Number) value.getField(weightCol)).doubleValue();
            out.collect(Tuple3.of(prob, label == 1.0, weight));
        }
    }

    /** Binary Summary of data in one worker. */
    public static class BinarySummary implements Serializable {
        public Integer taskId;
        // maximum score in this partition
        public double maxScore;
        // real positives in this partition
        public long curPositive;
        // real negatives in this partition
        public long curNegative;

        public BinarySummary() {}

        public BinarySummary(Integer taskId, double maxScore, long curPositive, long curNegative) {
            this.taskId = taskId;
            this.maxScore = maxScore;
            this.curPositive = curPositive;
            this.curNegative = curNegative;
        }
    }
}
