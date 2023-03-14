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

package org.apache.flink.ml.classification.gbtclassifier;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ml.common.gbt.GBTModelData;
import org.apache.flink.ml.linalg.DenseVector;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.servable.api.DataFrame;
import org.apache.flink.ml.servable.api.ModelServable;
import org.apache.flink.ml.servable.api.Row;
import org.apache.flink.ml.servable.types.BasicType;
import org.apache.flink.ml.servable.types.DataTypes;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ServableReadWriteUtils;
import org.apache.flink.util.Preconditions;

import org.apache.commons.math3.analysis.function.Sigmoid;
import org.eclipse.collections.impl.map.mutable.primitive.IntDoubleHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Servable which can be used to classifies data in online inference. */
public class GBTClassifierModelServable
        implements ModelServable<GBTClassifierModelServable>,
                GBTClassifierModelParams<GBTClassifierModelServable> {

    private static final Sigmoid sigmoid = new Sigmoid();

    private final Map<Param<?>, Object> paramMap = new HashMap<>();

    private GBTModelData modelData;

    public GBTClassifierModelServable() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    GBTClassifierModelServable(GBTModelData modelData) {
        this();
        this.modelData = modelData;
    }

    public static GBTClassifierModelServable load(String path) throws IOException {
        GBTClassifierModelServable servable =
                ServableReadWriteUtils.loadServableParam(path, GBTClassifierModelServable.class);

        // See BaseGBTModel#MODEL_DATA_PATH
        final String modelDataPath = "model_data";
        try (InputStream modelData =
                ServableReadWriteUtils.loadModelData(path + "/" + modelDataPath)) {
            servable.setModelData(modelData);
            return servable;
        }
    }

    @Override
    public DataFrame transform(DataFrame input) {
        List<Double> predictions = new ArrayList<>();
        List<DenseVector> rawPredictions = new ArrayList<>();
        List<DenseVector> probabilities = new ArrayList<>();

        String[] featuresCols = getFeaturesCols();
        Integer[] featuresColIndices =
                Arrays.stream(featuresCols).map(input::getIndex).toArray(Integer[]::new);

        for (Row row : input.collect()) {
            IntDoubleHashMap features = modelData.toFeatures(featuresColIndices, row::get);
            Tuple3<Double, DenseVector, DenseVector> results = transform(features);
            predictions.add(results.f0);
            rawPredictions.add(results.f1);
            probabilities.add(results.f2);
        }

        input.addColumn(getPredictionCol(), DataTypes.DOUBLE, predictions);
        input.addColumn(getRawPredictionCol(), DataTypes.VECTOR(BasicType.DOUBLE), rawPredictions);
        input.addColumn(getProbabilityCol(), DataTypes.VECTOR(BasicType.DOUBLE), probabilities);
        return input;
    }

    /**
     * The main logic that predicts one input data point.
     *
     * @param features The input features.
     * @return The prediction label, raw predictions, and probabilities.
     */
    protected Tuple3<Double, DenseVector, DenseVector> transform(IntDoubleHashMap features) {
        double logits = modelData.predictRaw(features);
        double prob = sigmoid.value(logits);
        return Tuple3.of(
                logits >= 0. ? 1. : 0.,
                Vectors.dense(-logits, logits),
                Vectors.dense(1 - prob, prob));
    }

    public GBTClassifierModelServable setModelData(InputStream... modelDataInputs)
            throws IOException {
        Preconditions.checkArgument(modelDataInputs.length == 1);
        modelData = GBTModelData.decode(modelDataInputs[0]);
        return this;
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
