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

package org.apache.flink.ml.clustering.kmeans;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.Estimator;
import org.apache.flink.ml.common.computation.builder.Data;
import org.apache.flink.ml.common.computation.computation.CompositeComputation;
import org.apache.flink.ml.common.computation.computation.IterationComputation;
import org.apache.flink.ml.common.computation.purefunc.MapPureFunc;
import org.apache.flink.ml.common.computation.purefunc.MapWithDataPureFunc;
import org.apache.flink.ml.common.computation.purefunc.ReducePureFunc;
import org.apache.flink.ml.common.distance.DistanceMeasure;
import org.apache.flink.ml.linalg.BLAS;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vector;
import org.apache.flink.ml.linalg.VectorWithNorm;
import org.apache.flink.ml.linalg.typeinfo.DenseVectorTypeInfo;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** KMeans. */
public class KMeans2 implements Estimator<KMeans2, KMeansModel>, KMeansParams<KMeans2> {

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    public static IterationComputation makeIterationComputation(
            int maxIters, DistanceMeasure distanceMeasure) {
        Data<DenseVector[]> centroids =
                new Data<>(Types.OBJECT_ARRAY(DenseVectorTypeInfo.INSTANCE));
        Data<DenseVector> points = new Data<>(DenseVectorTypeInfo.INSTANCE);

        Data<VectorWithNorm> pointsWithNorm =
                points.map(
                        (value, out) -> out.collect(new VectorWithNorm(value)),
                        TypeInformation.of(VectorWithNorm.class));

        Data<Iterable<VectorWithNorm>> cachedPoints = pointsWithNorm.cacheWithSequentialRead();

        Data<Tuple2<Integer[], DenseVector[]>> centroidIdAndPoints =
                centroids.map(
                        new CentroidsUpdatePureFunc(distanceMeasure),
                        cachedPoints,
                        Types.TUPLE(
                                Types.OBJECT_ARRAY(Types.INT),
                                Types.OBJECT_ARRAY(DenseVectorTypeInfo.INSTANCE)));

        Data<KMeansModelData> newModelData =
                centroidIdAndPoints
                        .reduce(new CentroidsUpdateReducer())
                        .map(new ModelDataGenerator(), TypeInformation.of(KMeansModelData.class));

        Data<DenseVector[]> newCentroids =
                newModelData.map(
                        (value, out) -> out.collect(value.centroids),
                        Types.OBJECT_ARRAY(DenseVectorTypeInfo.INSTANCE));

        Data<Boolean> endCriteria =
                newModelData.map(
                        (value, iteration, out) -> out.collect(iteration >= maxIters),
                        Types.BOOLEAN);

        Data<KMeansModelData> outputModel =
                newModelData.map(
                        (value, isFinal, out) -> {
                            if (isFinal) {
                                out.collect(value);
                            }
                        },
                        endCriteria,
                        newModelData.type);
        return new IterationComputation(
                new CompositeComputation(
                        Arrays.asList(centroids, points),
                        Arrays.asList(endCriteria, newCentroids, outputModel)),
                Collections.emptyList(),
                Collections.singletonMap(1, 0),
                0,
                Collections.singletonList(2));
    }

    @Override
    public KMeansModel fit(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);

        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        DataStream<DenseVector> points =
                tEnv.toDataStream(inputs[0])
                        .map(row -> ((Vector) row.getField(getFeaturesCol())).toDense());

        DataStream<DenseVector[]> initCentroids =
                KMeans.selectRandomCentroids(points, getK(), getSeed());

        //        IterationConfig config =
        //                IterationConfig.newBuilder()
        //                        .setOperatorLifeCycle(IterationConfig.OperatorLifeCycle.ALL_ROUND)
        //                        .build();
        //
        //        Data<DenseVector[]> initCentroidsData = new Data<>(initCentroids.getType());
        //        Data<DenseVector> pointsData = new Data<>(points.getType());
        //
        //        IterationBody body =
        //                new KMeans.KMeansIterationBody(
        //                        getMaxIter(), DistanceMeasure.getInstance(getDistanceMeasure()));
        //
        //        DataStream<KMeansModelData> finalModelData =
        //                Iterations.iterateBoundedStreamsUntilTermination(
        //                                DataStreamList.of(initCentroids),
        //                                ReplayableDataStreamList.notReplay(points),
        //                                config,
        //                                body)
        //                        .get(0);

        IterationComputation iterationComputation =
                makeIterationComputation(
                        getMaxIter(), DistanceMeasure.getInstance(getDistanceMeasure()));
        List<DataStream<?>> outputs = iterationComputation.executeOnFlink(initCentroids, points);
        //noinspection unchecked
        DataStream<KMeansModelData> finalModelData = (DataStream<KMeansModelData>) outputs.get(0);

        Table finalModelDataTable = tEnv.fromDataStream(finalModelData);
        KMeansModel model = new KMeansModel().setModelData(finalModelDataTable);
        ReadWriteUtils.updateExistingParams(model, paramMap);
        return model;
    }

    @Override
    public void save(String path) throws IOException {}

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }

    static class CentroidsUpdatePureFunc
            implements MapWithDataPureFunc<
                    DenseVector[], Iterable<VectorWithNorm>, Tuple2<Integer[], DenseVector[]>> {

        private final DistanceMeasure distanceMeasure;

        CentroidsUpdatePureFunc(DistanceMeasure distanceMeasure) {
            this.distanceMeasure = distanceMeasure;
        }

        @Override
        public void map(
                DenseVector[] centroids,
                Iterable<VectorWithNorm> points,
                Collector<Tuple2<Integer[], DenseVector[]>> out) {
            VectorWithNorm[] centroidsWithNorm = new VectorWithNorm[centroids.length];
            for (int i = 0; i < centroidsWithNorm.length; i++) {
                centroidsWithNorm[i] = new VectorWithNorm(centroids[i]);
            }

            DenseVector[] newCentroids = new DenseVector[centroids.length];
            Integer[] counts = new Integer[centroids.length];
            Arrays.fill(counts, 0);
            for (int i = 0; i < centroids.length; i++) {
                newCentroids[i] = new DenseVector(centroids[i].size());
            }

            for (VectorWithNorm point : points) {
                int closestCentroidId = distanceMeasure.findClosest(centroidsWithNorm, point);
                BLAS.axpy(1.0, point.vector, newCentroids[closestCentroidId]);
                counts[closestCentroidId]++;
            }
            out.collect(Tuple2.of(counts, newCentroids));
        }
    }

    static class CentroidsUpdateReducer
            implements ReducePureFunc<Tuple2<Integer[], DenseVector[]>> {
        @Override
        public Tuple2<Integer[], DenseVector[]> reduce(
                Tuple2<Integer[], DenseVector[]> tuple2, Tuple2<Integer[], DenseVector[]> t1)
                throws Exception {
            for (int i = 0; i < tuple2.f0.length; i++) {
                tuple2.f0[i] += t1.f0[i];
                BLAS.axpy(1.0, t1.f1[i], tuple2.f1[i]);
            }
            return tuple2;
        }
    }

    private static class ModelDataGenerator
            implements MapPureFunc<Tuple2<Integer[], DenseVector[]>, KMeansModelData> {
        @Override
        public void map(Tuple2<Integer[], DenseVector[]> value, Collector<KMeansModelData> out) {
            double[] weights = new double[value.f0.length];
            for (int i = 0; i < value.f0.length; i++) {
                BLAS.scal(1.0 / value.f0[i], value.f1[i]);
                weights[i] = value.f0[i];
            }
            out.collect(new KMeansModelData(value.f1, new DenseVector(weights)));
        }
    }
}
