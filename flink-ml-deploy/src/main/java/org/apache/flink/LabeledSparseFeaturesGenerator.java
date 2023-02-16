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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.benchmark.datagenerator.common.InputTableGenerator;
import org.apache.flink.ml.benchmark.datagenerator.common.RowGenerator;
import org.apache.flink.ml.benchmark.datagenerator.param.HasVectorDim;
import org.apache.flink.ml.linalg.Vectors;
import org.apache.flink.ml.linalg.typeinfo.SparseVectorTypeInfo;
import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

/**
 * A DataGenerator which creates a table of features and label.
 *
 * <p>Users need to specify two column names as {@link #COL_NAMES}'s value in the following order:
 *
 * <ul>
 *   <li>features column name
 *   <li>label column name
 * </ul>
 */
public class LabeledSparseFeaturesGenerator
        extends InputTableGenerator<LabeledSparseFeaturesGenerator>
        implements HasVectorDim<LabeledSparseFeaturesGenerator> {

    public static final Param<Integer> FEATURE_NNZ =
            new IntParam(
                    "featureNnz",
                    "Number of non-zero values in the range [1, featureArity]. ",
                    1,
                    ParamValidators.gt(0));

    public static final Param<Integer> LABEL_ARITY =
            new IntParam(
                    "labelArity",
                    "Arity of label. "
                            + "If set to positive value, the label would be an integer in range [0, arity - 1]. "
                            + "If set to zero, the label would be a continuous double in range [0, 1).",
                    2,
                    ParamValidators.gtEq(0));

    public LabeledSparseFeaturesGenerator() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    public int getFeatureNnz() {
        return get(FEATURE_NNZ);
    }

    public LabeledSparseFeaturesGenerator setFeatureNnz(int value) {
        return set(FEATURE_NNZ, value);
    }

    public int getLabelArity() {
        return get(LABEL_ARITY);
    }

    public LabeledSparseFeaturesGenerator setLabelArity(int value) {
        return set(LABEL_ARITY, value);
    }

    @Override
    protected RowGenerator[] getRowGenerators() {
        String[][] colNames = getColNames();
        Preconditions.checkState(colNames.length == 1);
        Preconditions.checkState(colNames[0].length == 2);
        int vectorDim = getVectorDim();
        int labelArity = getLabelArity();
        int nnz = getFeatureNnz();

        return new RowGenerator[] {
            new RowGenerator(getNumValues(), getSeed()) {
                @Override
                protected Row getRow() {
                    List<Integer> indices = new ArrayList<>(nnz);
                    List<Double> values = new ArrayList<>(nnz);
                    for (int i = 0; i < vectorDim; i += 1) {
                        if (random.nextDouble() <= 1. * nnz / vectorDim) {
                            indices.add(i);
                            values.add(random.nextDouble());
                        }
                    }

                    double label = getValue(labelArity);
                    return Row.of(
                            Vectors.sparse(
                                    vectorDim,
                                    indices.stream().mapToInt(d -> d).toArray(),
                                    values.stream().mapToDouble(d -> d).toArray()),
                            label);
                }

                @Override
                protected RowTypeInfo getRowTypeInfo() {
                    return new RowTypeInfo(
                            new TypeInformation[] {SparseVectorTypeInfo.INSTANCE, Types.DOUBLE},
                            colNames[0]);
                }

                private double getValue(int arity) {
                    if (arity > 0) {
                        return random.nextInt(arity);
                    }
                    return random.nextDouble();
                }
            }
        };
    }
}
