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

package org.apache.flink.ml.common.gbt.defs;

import org.apache.flink.util.Preconditions;

/**
 * This class stores splits of nodes in the current layer, and necessary information of
 * all-reducing.
 */
public class Splits {
    // Stores (nodeId, featureId) pair index.
    public int pairId;
    // Stores node index in the current layer.
    public int nodeId;
    // Stores split of (nodeId, featureId) pair.
    public Split split;

    public Splits() {}

    public Splits(int pairId, int nodeId, Split split) {
        this.pairId = pairId;
        this.nodeId = nodeId;
        this.split = split;
    }

    public Splits accumulate(Splits other) {
        Preconditions.checkArgument(nodeId == other.nodeId);
        if (split == null && other.split != null) {
            split = other.split;
        } else if (split != null && other.split != null) {
            if (split.gain < other.split.gain) {
                split = other.split;
            } else if (split.gain == other.split.gain) {
                if (split.featureId < other.split.featureId) {
                    split = other.split;
                }
            }
        }
        return this;
    }
}
