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

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.ml.common.gbt.defs.Histogram;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/** Aggregation function for merging histograms. */
public class HistogramAggregateFunction extends RichFlatMapFunction<Histogram, Histogram> {

    private final Map<Integer, BitSet> pairAccepted = new HashMap<>();
    private final Map<Integer, Histogram> pairAcc = new HashMap<>();
    private int numSubtasks;

    @Override
    public void open(Configuration parameters) throws Exception {
        numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    }

    @Override
    public void flatMap(Histogram value, Collector<Histogram> out) throws Exception {
        int pairId = value.pairId;
        int fromSubtaskId = value.subtaskId;

        BitSet accepted = pairAccepted.getOrDefault(pairId, new BitSet(numSubtasks));
        Preconditions.checkState(!accepted.get(fromSubtaskId));
        accepted.set(fromSubtaskId);
        pairAccepted.put(pairId, accepted);

        pairAcc.compute(pairId, (k, v) -> null == v ? value : v.accumulate(value));
        if (numSubtasks == accepted.cardinality()) {
            Histogram acc = pairAcc.get(pairId);
            acc.subtaskId = getRuntimeContext().getIndexOfThisSubtask();
            out.collect(acc);
            pairAccepted.remove(pairId);
            pairAcc.remove(pairId);
        }
    }
}
