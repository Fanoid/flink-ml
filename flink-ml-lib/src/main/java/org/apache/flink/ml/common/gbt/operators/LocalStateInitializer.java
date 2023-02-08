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

package org.apache.flink.ml.common.gbt.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.common.gbt.defs.BinnedInstance;
import org.apache.flink.ml.common.gbt.defs.GbtParams;
import org.apache.flink.ml.common.gbt.defs.LocalState;
import org.apache.flink.ml.common.gbt.defs.TaskType;
import org.apache.flink.ml.common.gbt.loss.AbsoluteError;
import org.apache.flink.ml.common.gbt.loss.LogLoss;
import org.apache.flink.ml.common.gbt.loss.Loss;
import org.apache.flink.ml.common.gbt.loss.SquaredError;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import static java.util.Arrays.stream;

class LocalStateInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStateInitializer.class);
    private final GbtParams params;

    public LocalStateInitializer(GbtParams params) {
        this.params = params;
    }

    /**
     * Initializes local state.
     *
     * <p>Note that local state already has some properties set in advance, see GBTRunner#boost.
     */
    public LocalState init(
            LocalState localState, int subtaskId, int numSubtasks, BinnedInstance[] instances) {
        LOG.info("subtaskId: {}, {} start", subtaskId, LocalStateInitializer.class.getSimpleName());

        LocalState.Statics statics = localState.statics;
        statics.subtaskId = subtaskId;
        statics.numSubtasks = numSubtasks;

        int numInstances = instances.length;
        int numFeatures = statics.featureMetas.length;

        LOG.info(
                "subtaskId: {}, #samples: {}, #features: {}", subtaskId, numInstances, numFeatures);

        statics.params = params;
        statics.numInstances = numInstances;
        statics.numFeatures = numFeatures;

        statics.numBaggingInstances = getNumBaggingSamples(numInstances);
        statics.numBaggingFeatures = getNumBaggingFeatures(numFeatures);

        statics.instanceRandomizer = new Random(subtaskId + params.seed);
        statics.featureRandomizer = new Random(params.seed);

        statics.loss = getLoss();
        statics.prior = calcPrior(statics.labelSumCount);

        statics.numFeatureBins =
                stream(statics.featureMetas)
                        .mapToInt(d -> d.numBins(statics.params.useMissing))
                        .toArray();

        LocalState.Dynamics dynamics = localState.dynamics;
        dynamics.inWeakLearner = false;

        LOG.info("subtaskId: {}, {} end", subtaskId, LocalStateInitializer.class.getSimpleName());
        return new LocalState(statics, dynamics);
    }

    private int getNumBaggingSamples(int numSamples) {
        return (int) Math.min(numSamples, Math.ceil(numSamples * params.subsamplingRate));
    }

    private int getNumBaggingFeatures(int numFeatures) {
        final List<String> supported = Arrays.asList("auto", "all", "onethird", "sqrt", "log2");
        final String errorMsg =
                String.format(
                        "Parameter `featureSubsetStrategy` supports %s, (0.0 - 1.0], [1 - n].",
                        String.join(", ", supported));
        final Function<Double, Integer> clamp =
                d -> Math.max(1, Math.min(d.intValue(), numFeatures));
        String strategy = params.featureSubsetStrategy;
        try {
            int numBaggingFeatures = Integer.parseInt(strategy);
            Preconditions.checkArgument(
                    numBaggingFeatures >= 1 && numBaggingFeatures <= numFeatures, errorMsg);
        } catch (NumberFormatException ignored) {
        }
        try {
            double baggingRatio = Double.parseDouble(strategy);
            Preconditions.checkArgument(baggingRatio > 0. && baggingRatio <= 1., errorMsg);
            return clamp.apply(baggingRatio * numFeatures);
        } catch (NumberFormatException ignored) {
        }

        Preconditions.checkArgument(supported.contains(strategy), errorMsg);
        switch (strategy) {
            case "auto":
                return TaskType.CLASSIFICATION.equals(params.taskType)
                        ? clamp.apply(Math.sqrt(numFeatures))
                        : clamp.apply(numFeatures / 3.);
            case "all":
                return numFeatures;
            case "onethird":
                return clamp.apply(numFeatures / 3.);
            case "sqrt":
                return clamp.apply(Math.sqrt(numFeatures));
            case "log2":
                return clamp.apply(Math.log(numFeatures) / Math.log(2));
            default:
                throw new IllegalArgumentException(errorMsg);
        }
    }

    private Loss getLoss() {
        String lossType = params.lossType;
        switch (lossType) {
            case "logistic":
                return LogLoss.INSTANCE;
            case "squared":
                return SquaredError.INSTANCE;
            case "absolute":
                return AbsoluteError.INSTANCE;
            default:
                throw new UnsupportedOperationException("Unsupported loss.");
        }
    }

    private double calcPrior(Tuple2<Double, Long> labelStat) {
        String lossType = params.lossType;
        switch (lossType) {
            case "logistic":
                return Math.log(labelStat.f0 / (labelStat.f1 - labelStat.f0));
            case "squared":
                return labelStat.f0 / labelStat.f1;
            case "absolute":
                throw new UnsupportedOperationException("absolute error is not supported yet.");
            default:
                throw new UnsupportedOperationException("Unsupported loss.");
        }
    }
}
