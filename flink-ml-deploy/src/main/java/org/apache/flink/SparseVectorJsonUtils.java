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

package org.apache.flink;

import org.apache.flink.ml.linalg.SparseVector;
import org.apache.flink.table.functions.ScalarFunction;

import org.apache.commons.lang3.StringUtils;

/** Hi! */
public class SparseVectorJsonUtils {

    /** Delimiter between elements. */
    private static final char ELEMENT_DELIMITER = ' ';
    /** Delimiter between vector size and vector data. */
    private static final char HEADER_DELIMITER = '$';
    /** Delimiter between index and value. */
    private static final char INDEX_VALUE_DELIMITER = ':';

    static String serialize(SparseVector sv) {
        StringBuilder sbd = new StringBuilder();
        if (sv.n > 0) {
            sbd.append(HEADER_DELIMITER);
            sbd.append(sv.n);
            sbd.append(HEADER_DELIMITER);
        }
        if (null != sv.indices) {
            for (int i = 0; i < sv.indices.length; i++) {
                sbd.append(sv.indices[i]);
                sbd.append(INDEX_VALUE_DELIMITER);
                sbd.append(sv.values[i]);
                if (i < sv.indices.length - 1) {
                    sbd.append(ELEMENT_DELIMITER);
                }
            }
        }
        return sbd.toString();
    }

    public static SparseVector parseSparse(String str, int vectorSize) {
        try {
            int lastDollarPos;
            int n;
            if (vectorSize > 0) {
                n = vectorSize;
                lastDollarPos = -1;
            } else {
                int firstDollarPos = str.indexOf(HEADER_DELIMITER);
                lastDollarPos = str.lastIndexOf(HEADER_DELIMITER);
                String sizeStr = str.substring(firstDollarPos + 1, lastDollarPos);
                n = Integer.parseInt(sizeStr);
            }
            int numValues = StringUtils.countMatches(str, String.valueOf(INDEX_VALUE_DELIMITER));
            double[] data = new double[numValues];
            int[] indices = new int[numValues];
            int startPos = lastDollarPos + 1;
            int endPos;
            for (int i = 0; i < numValues; i++) {
                int colonPos = str.indexOf(INDEX_VALUE_DELIMITER, startPos);
                endPos = str.indexOf(ELEMENT_DELIMITER, colonPos);
                if (endPos == -1) {
                    endPos = str.length();
                }
                indices[i] = Integer.parseInt(str.substring(startPos, colonPos));
                data[i] = Double.parseDouble(str.substring(colonPos + 1, endPos));
                startPos = endPos + 1;
            }
            return new SparseVector(n, indices, data);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format("Fail to getVector sparse vector from string: \"%s\".", str), e);
        }
    }

    /** Deserialize sparse vector. */
    public static class DeserializeSparseVector extends ScalarFunction {
        public SparseVector eval(String s, int vectorSize) {
            return parseSparse(s, vectorSize);
        }
    }
}
