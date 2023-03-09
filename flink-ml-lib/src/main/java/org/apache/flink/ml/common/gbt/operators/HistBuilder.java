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
import org.apache.flink.ml.common.gbt.DataUtils;
import org.apache.flink.ml.common.gbt.defs.BinnedInstance;
import org.apache.flink.ml.common.gbt.defs.FeatureMeta;
import org.apache.flink.ml.common.gbt.defs.Histogram;
import org.apache.flink.ml.common.gbt.defs.LearningNode;
import org.apache.flink.ml.common.gbt.defs.Slice;
import org.apache.flink.ml.common.gbt.defs.TrainContext;
import org.apache.flink.ml.util.Distributor;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.apache.flink.ml.common.gbt.DataUtils.BIN_SIZE;

class HistBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(HistBuilder.class);
    private final int subtaskId;
    private final int numSubtasks;
    private final int[] numFeatureBins;
    private final FeatureMeta[] featureMetas;
    private final int numBaggingFeatures;
    private final Random featureRandomizer;
    private final int[] featureIndicesPool;
    private final boolean isInputVector;
    private final int maxFeatureBins;
    private final int totalNumFeatureBins;

    public HistBuilder(TrainContext trainContext) {
        subtaskId = trainContext.subtaskId;
        numSubtasks = trainContext.numSubtasks;

        numFeatureBins = trainContext.numFeatureBins;
        featureMetas = trainContext.featureMetas;

        numBaggingFeatures = trainContext.numBaggingFeatures;
        featureRandomizer = trainContext.featureRandomizer;
        featureIndicesPool = IntStream.range(0, trainContext.numFeatures).toArray();

        isInputVector = trainContext.strategy.isInputVector;

        maxFeatureBins = Arrays.stream(numFeatureBins).max().orElse(0);
        totalNumFeatureBins = Arrays.stream(numFeatureBins).sum();
    }

    /**
     * Calculate histograms for all (nodeId, featureId) pairs. The results are written to `hists`,
     * so `hists` must be large enough to store values.
     */
    private static void calcNodeFeaturePairHists(
            List<LearningNode> layer,
            int[][] nodeToFeatures,
            FeatureMeta[] featureMetas,
            int[] numFeatureBins,
            boolean isInputVector,
            int[] indices,
            BinnedInstance[] instances,
            double[] pgh,
            double[] hists,
            ArrayBlockingQueue<Callable<Void>> tasks) {
        int numNodes = layer.size();
        int numFeatures = featureMetas.length;

        int[][] nodeToBinOffsets = new int[numNodes][];
        int[] nodeBinSizes = new int[numNodes];
        int binOffset = 0;
        for (int k = 0; k < numNodes; k += 1) {
            int[] features = nodeToFeatures[k];
            nodeToBinOffsets[k] = new int[features.length];
            for (int i = 0; i < features.length; i += 1) {
                nodeToBinOffsets[k][i] = binOffset;
                binOffset += numFeatureBins[features[i]];
                nodeBinSizes[k] += numFeatureBins[features[i]];
            }
        }

        int[] featureDefaultVal = new int[numFeatures];
        for (int i = 0; i < numFeatures; i += 1) {
            FeatureMeta d = featureMetas[i];
            featureDefaultVal[i] =
                    isInputVector && d instanceof FeatureMeta.ContinuousFeatureMeta
                            ? ((FeatureMeta.ContinuousFeatureMeta) d).zeroBin
                            : d.missingBin;
        }

        long start = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(numNodes);
        for (int nodeId = 0; nodeId < numNodes; nodeId += 1) {
            int[] featureOffset = new int[numFeatures];
            BitSet featureValid = null;
            boolean allFeatureValid;

            int[] features = nodeToFeatures[nodeId];
            int[] binOffsets = nodeToBinOffsets[nodeId];
            LearningNode node = layer.get(nodeId);

            if (numFeatures != features.length) {
                allFeatureValid = false;
                featureValid = new BitSet(numFeatures);
                for (int feature : features) {
                    featureValid.set(feature);
                }
                for (int i = 0; i < features.length; i += 1) {
                    featureOffset[features[i]] = binOffsets[i];
                }
            } else {
                allFeatureValid = true;
                System.arraycopy(binOffsets, 0, featureOffset, 0, numFeatures);
            }
            int nodeOffset = binOffsets[0];

            final int granularity = 100000;
            final int numThreadTasks = (node.slice.size() - 1) / granularity + 1;
            ConcurrentLinkedQueue<SliceHist> sliceHists = new ConcurrentLinkedQueue<>();
            Distributor distributor =
                    new Distributor.EvenDistributor(numThreadTasks, node.slice.size());
            for (int k = 0; k < numThreadTasks; k += 1) {
                // Every thread task calculates histograms of a slice of instances.
                final Slice slice =
                        new Slice(
                                node.slice.start + (int) distributor.start(k),
                                node.slice.start
                                        + (int) (distributor.start(k) + distributor.count(k)));
                tasks.add(
                        new CalcSliceHistTask(
                                slice,
                                nodeId,
                                numThreadTasks,
                                indices,
                                instances,
                                pgh,
                                allFeatureValid,
                                featureValid,
                                featureOffset,
                                nodeBinSizes[nodeId],
                                nodeOffset,
                                sliceHists,
                                tasks,
                                features,
                                featureDefaultVal,
                                numFeatureBins,
                                hists,
                                countDownLatch));
            }
        }
        LOG.info("countDownLatch.await()");
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Preconditions.checkState(tasks.isEmpty());
        LOG.info("STEP 3: {} ms", System.currentTimeMillis() - start);
    }

    private static void add(
            double[] hists, int offset, int val, double d0, double d1, double d2, double d3) {
        int index = (offset + val) * BIN_SIZE;
        hists[index] += d0;
        hists[index + 1] += d1;
        hists[index + 2] += d2;
        hists[index + 3] += d3;
    }

    private static void add(double[] hists, int offset, int val, double[] src, int srcOffset) {
        int index = (offset + val) * BIN_SIZE;
        hists[index] += src[srcOffset];
        hists[index + 1] += src[srcOffset + 1];
        hists[index + 2] += src[srcOffset + 2];
        hists[index + 3] += src[srcOffset + 3];
    }

    private static void minus(double[] hists, int offset, int val, double[] src, int srcOffset) {
        int index = (offset + val) * BIN_SIZE;
        hists[index] -= src[srcOffset];
        hists[index + 1] -= src[srcOffset + 1];
        hists[index + 2] -= src[srcOffset + 2];
        hists[index + 3] -= src[srcOffset + 3];
    }

    /**
     * Calculates elements counts of histogram distributed to each downstream subtask. The elements
     * counts is bin counts multiplied by STEP. The minimum unit to be distributed is (nodeId,
     * featureId), i.e., all bins belonging to the same (nodeId, featureId) pair must go to one
     * subtask.
     */
    private static int[] calcRecvCounts(
            int numSubtasks, int[] nodeFeaturePairs, int[] numFeatureBins) {
        int[] recvcnts = new int[numSubtasks];
        Distributor.EvenDistributor distributor =
                new Distributor.EvenDistributor(numSubtasks, nodeFeaturePairs.length / 2);
        for (int k = 0; k < numSubtasks; k += 1) {
            int pairStart = (int) distributor.start(k);
            int pairCnt = (int) distributor.count(k);
            for (int i = pairStart; i < pairStart + pairCnt; i += 1) {
                int featureId = nodeFeaturePairs[2 * i + 1];
                recvcnts[k] += numFeatureBins[featureId] * BIN_SIZE;
            }
        }
        return recvcnts;
    }

    /** Calculate local histograms for nodes in current layer of tree. */
    List<Tuple2<Integer, Histogram>> build(
            List<LearningNode> layer,
            int[] indices,
            BinnedInstance[] instances,
            double[] pgh,
            Consumer<int[]> nodeFeaturePairsSetter,
            ArrayBlockingQueue<Callable<Void>> tasks) {
        LOG.info("subtaskId: {}, {} start", subtaskId, HistBuilder.class.getSimpleName());
        int numNodes = layer.size();

        // Generates (nodeId, featureId) pairs that are required to build histograms.
        int[][] nodeToFeatures = new int[numNodes][];
        int[] nodeFeaturePairs = new int[numNodes * numBaggingFeatures * 2];
        int p = 0;
        for (int k = 0; k < numNodes; k += 1) {
            nodeToFeatures[k] =
                    DataUtils.sample(featureIndicesPool, numBaggingFeatures, featureRandomizer);
            Arrays.sort(nodeToFeatures[k]);
            for (int featureId : nodeToFeatures[k]) {
                nodeFeaturePairs[p++] = k;
                nodeFeaturePairs[p++] = featureId;
            }
        }
        nodeFeaturePairsSetter.accept(nodeFeaturePairs);

        int maxNumBins =
                numNodes * Math.min(maxFeatureBins * numBaggingFeatures, totalNumFeatureBins);
        double[] hists = new double[maxNumBins * BIN_SIZE];
        // Calculates histograms for (nodeId, featureId) pairs.
        long start = System.currentTimeMillis();
        calcNodeFeaturePairHists(
                layer,
                nodeToFeatures,
                featureMetas,
                numFeatureBins,
                isInputVector,
                indices,
                instances,
                pgh,
                hists,
                tasks);
        long elapsed = System.currentTimeMillis() - start;
        LOG.info("Elapsed time for calcNodeFeaturePairHists: {} ms", elapsed);

        // Calculates number of elements received by each downstream subtask.
        int[] recvcnts = calcRecvCounts(numSubtasks, nodeFeaturePairs, numFeatureBins);

        List<Tuple2<Integer, Histogram>> histograms = new ArrayList<>();
        int sliceStart = 0;
        for (int i = 0; i < recvcnts.length; i += 1) {
            int sliceSize = recvcnts[i];
            histograms.add(
                    Tuple2.of(
                            i,
                            new Histogram(
                                    subtaskId,
                                    hists,
                                    new Slice(sliceStart, sliceStart + sliceSize))));
            sliceStart += sliceSize;
        }

        LOG.info("subtaskId: {}, {} end", this.subtaskId, HistBuilder.class.getSimpleName());
        return histograms;
    }

    private static class CalcSliceHistTask implements Callable<Void> {
        private final Slice slice;
        private final int nodeId;
        private final int totalNumSlices;
        private final int[] indices;
        private final BinnedInstance[] instances;
        private final double[] pgh;
        private final boolean allFeatureValid;
        private final BitSet featureValid;
        private final int[] featureOffset;
        private final int nodeBinSize;
        private final int nodeOffset;
        private final ConcurrentLinkedQueue<SliceHist> sliceResults;
        private final ArrayBlockingQueue<Callable<Void>> tasks;
        private final int[] features;
        private final int[] featureDefaultVal;
        private final int[] numFeatureBins;
        private final double[] hists;
        private final CountDownLatch countDownLatch;

        CalcSliceHistTask(
                Slice slice,
                int nodeId,
                int totalNumSlices,
                int[] indices,
                BinnedInstance[] instances,
                double[] pgh,
                boolean allFeatureValid,
                BitSet featureValid,
                int[] featureOffset,
                int nodeBinSize,
                int nodeOffset,
                ConcurrentLinkedQueue<SliceHist> sliceResults,
                ArrayBlockingQueue<Callable<Void>> tasks,
                int[] features,
                int[] featureDefaultVal,
                int[] numFeatureBins,
                double[] hists,
                CountDownLatch countDownLatch) {
            this.slice = slice;
            this.nodeId = nodeId;
            this.totalNumSlices = totalNumSlices;
            this.indices = indices;
            this.instances = instances;
            this.pgh = pgh;
            this.allFeatureValid = allFeatureValid;
            this.featureValid = featureValid;
            this.featureOffset = featureOffset;
            this.nodeBinSize = nodeBinSize;
            this.nodeOffset = nodeOffset;
            this.sliceResults = sliceResults;
            this.tasks = tasks;
            this.features = features;
            this.featureDefaultVal = featureDefaultVal;
            this.numFeatureBins = numFeatureBins;
            this.hists = hists;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public Void call() {
            double[] localHists = new double[nodeBinSize * 4];
            double[] localTotalHists = new double[4];

            for (int i = slice.start; i < slice.end; i += 1) {
                int instanceId = indices[i];
                BinnedInstance binnedInstance = instances[instanceId];
                double weight = binnedInstance.weight;
                double gradient = pgh[3 * instanceId + 1];
                double hessian = pgh[3 * instanceId + 2];

                localTotalHists[0] += gradient;
                localTotalHists[1] += hessian;
                localTotalHists[2] += weight;
                localTotalHists[3] += 1.;
            }

            for (int i = slice.start; i < slice.end; i += 1) {
                int instanceId = indices[i];
                BinnedInstance binnedInstance = instances[instanceId];
                double weight = binnedInstance.weight;
                double gradient = pgh[3 * instanceId + 1];
                double hessian = pgh[3 * instanceId + 2];

                if (null == binnedInstance.featureIds) {
                    for (int j = 0; j < binnedInstance.featureValues.length; j += 1) {
                        if (allFeatureValid || featureValid.get(j)) {
                            add(
                                    localHists,
                                    featureOffset[j] - nodeOffset,
                                    binnedInstance.featureValues[j],
                                    gradient,
                                    hessian,
                                    weight,
                                    1.);
                        }
                    }
                } else {
                    for (int j = 0; j < binnedInstance.featureIds.length; j += 1) {
                        int featureId = binnedInstance.featureIds[j];
                        if (allFeatureValid || featureValid.get(featureId)) {
                            add(
                                    localHists,
                                    featureOffset[featureId] - nodeOffset,
                                    binnedInstance.featureValues[j],
                                    gradient,
                                    hessian,
                                    weight,
                                    1.);
                        }
                    }
                }
            }

            SliceHist sliceHist = new SliceHist(1, totalNumSlices, localTotalHists, localHists);
            sliceResults.offer(sliceHist);
            LOG.debug(
                    "Put a slice hist for node {}, queue {}, current #slices {}, total #slices {}, {}",
                    nodeId,
                    sliceResults,
                    sliceHist.currentNumSlices,
                    sliceHist.totalNumSlices,
                    sliceHist);
            tasks.add(
                    new MergeHistsTask(
                            features,
                            featureDefaultVal,
                            featureOffset,
                            numFeatureBins,
                            hists,
                            sliceResults,
                            nodeOffset,
                            tasks,
                            countDownLatch));
            return null;
        }
    }

    private static class SliceHist {
        public int currentNumSlices;
        public int totalNumSlices;
        public double[] localTotalHists;
        public double[] localHists;

        public SliceHist(
                int currentNumSlices,
                int totalNumSlices,
                double[] localTotalHists,
                double[] localHists) {
            this.totalNumSlices = totalNumSlices;
            this.currentNumSlices = currentNumSlices;
            this.localTotalHists = localTotalHists;
            this.localHists = localHists;
        }
    }

    private static class MergeHistsTask implements Callable<Void> {
        private final int[] features;
        private final int[] featureDefaultVal;
        private final int[] featureOffset;
        private final int[] numFeatureBins;
        private final double[] hists;
        private final ConcurrentLinkedQueue<SliceHist> sliceResults;
        private final int nodeOffset;
        int nodeId;
        private final ArrayBlockingQueue<Callable<Void>> tasks;
        private final CountDownLatch countDownLatch;

        MergeHistsTask(
                int[] features,
                int[] featureDefaultVal,
                int[] featureOffset,
                int[] numFeatureBins,
                double[] hists,
                ConcurrentLinkedQueue<SliceHist> sliceResults,
                int nodeOffset,
                ArrayBlockingQueue<Callable<Void>> tasks,
                CountDownLatch countDownLatch) {
            this.features = features;
            this.featureDefaultVal = featureDefaultVal;
            this.featureOffset = featureOffset;
            this.numFeatureBins = numFeatureBins;
            this.hists = hists;
            this.sliceResults = sliceResults;
            this.nodeOffset = nodeOffset;
            this.tasks = tasks;
            this.countDownLatch = countDownLatch;
        }

        private void addToGlobalHists(SliceHist sliceHist) {
            double[] nodeHists = sliceHist.localHists;
            for (int featureId : features) {
                int defaultVal = featureDefaultVal[featureId];
                add(hists, featureOffset[featureId], defaultVal, sliceHist.localTotalHists, 0);
                for (int i = 0; i < numFeatureBins[featureId]; i += 1) {
                    if (i != defaultVal) {
                        int index = (i + featureOffset[featureId] - nodeOffset) * 4;
                        add(hists, featureOffset[featureId], i, nodeHists, index);
                        minus(hists, featureOffset[featureId], defaultVal, nodeHists, index);
                    }
                }
            }
            countDownLatch.countDown();
        }

        @Override
        public Void call() {
            SliceHist sliceHist = null;
            SliceHist other = null;

            synchronized (sliceResults) {
                if (sliceResults.size() == 1) {
                    SliceHist peek = sliceResults.peek();
                    if (peek.currentNumSlices == peek.totalNumSlices) {
                        sliceHist = sliceResults.poll();
                        LOG.debug(
                                "Got one slice hist for node {}, queue {}, current #slices {}, total #slices {}, {}",
                                nodeId,
                                sliceResults,
                                sliceHist.currentNumSlices,
                                sliceHist.totalNumSlices,
                                sliceHist);
                    }
                } else if (sliceResults.size() >= 2) {
                    sliceHist = sliceResults.poll();
                    LOG.debug(
                            "Got one slice hist for node {}, queue {}, current #slices {}, total #slices {}, {}",
                            nodeId,
                            sliceResults,
                            sliceHist.currentNumSlices,
                            sliceHist.totalNumSlices,
                            sliceHist);
                    other = sliceResults.poll();
                    LOG.debug(
                            "Got another slice hist for node {}, queue {}, current #slices {}, total #slices {}, {}",
                            nodeId,
                            sliceResults,
                            sliceHist.currentNumSlices,
                            sliceHist.totalNumSlices,
                            sliceHist);
                }
            }
            if (null != sliceHist) {
                if (null == other) {
                    Preconditions.checkState(
                            sliceHist.currentNumSlices == sliceHist.totalNumSlices);
                    addToGlobalHists(sliceHist);
                    return null;
                } else {
                    if (sliceHist.currentNumSlices == sliceHist.totalNumSlices) {
                        addToGlobalHists(sliceHist);
                        return null;
                    }
                    sliceHist.currentNumSlices += other.currentNumSlices;
                    for (int i = 0; i < sliceHist.localTotalHists.length; i += 1) {
                        sliceHist.localTotalHists[i] += other.localTotalHists[i];
                    }
                    for (int i = 0; i < sliceHist.localHists.length; i += 1) {
                        sliceHist.localHists[i] += other.localHists[i];
                    }
                    LOG.debug(
                            "Put a slice hist for node {}, queue {}, current #slices {}, total #slices {}, {}",
                            nodeId,
                            sliceResults,
                            sliceHist.currentNumSlices,
                            sliceHist.totalNumSlices,
                            sliceHist);
                    sliceResults.offer(sliceHist);
                }
            } else {
                tasks.add(
                        new MergeHistsTask(
                                features,
                                featureDefaultVal,
                                featureOffset,
                                numFeatureBins,
                                hists,
                                sliceResults,
                                nodeOffset,
                                tasks,
                                countDownLatch));
            }
            return null;
        }
    }
}
