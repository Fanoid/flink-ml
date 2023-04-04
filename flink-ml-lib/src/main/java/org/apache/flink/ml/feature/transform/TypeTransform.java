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

package org.apache.flink.ml.feature.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.ml.api.Transformer;
import org.apache.flink.ml.common.datastream.TableUtils;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.util.ParamUtils;
import org.apache.flink.ml.util.ReadWriteUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A Transformer that transform the types of special columns. */
public class TypeTransform
        implements Transformer<TypeTransform>, TypeTransformParams<TypeTransform> {
    private final Map<Param<?>, Object> paramMap = new HashMap<>();
    private static final String PREFIX = "typed_";

    public TypeTransform() {
        ParamUtils.initializeMapWithDefaultValues(paramMap, this);
    }

    @Override
    public Table[] transform(Table... inputs) {
        Preconditions.checkArgument(inputs.length == 1);
        StreamTableEnvironment tEnv =
                (StreamTableEnvironment) ((TableImpl) inputs[0]).getTableEnvironment();
        ResolvedSchema schema = inputs[0].getResolvedSchema();
        String[] allCols = schema.getColumnNames().toArray(new String[0]);
        List<String> toDoubleCols = Arrays.asList(getToDoubleCols());
        List<String> toFloatCols = Arrays.asList(getToFloatCols());
        List<String> toIntCols = Arrays.asList(getToIntCols());
        List<String> toLongCols = Arrays.asList(getToLongCols());
        List<String> toStringCols = Arrays.asList(getToStringCols());
        final boolean keepOrigin = getKeepOldCols();
        final double defaultDoubleVal = getDefaultDoubleValue();
        final float defaultFloatVal = getDefaultFloatValue();
        final long defaultLongVal = getDefaultLongValue();
        final int defaultIntVal = getDefaultIntValue();
        final String defaultStringVal = getDefaultStringValue();

        ApiExpression[] expressions =
                new ApiExpression
                        [allCols.length
                                + (keepOrigin
                                        ? (toDoubleCols.size()
                                                + toLongCols.size()
                                                + toStringCols.size()
                                                + toFloatCols.size()
                                                + toIntCols.size())
                                        : 0)];
        int iter = 0;
        if (keepOrigin) {
            for (String colName : allCols) {
                expressions[iter++] = Expressions.$(colName);
            }
        }

        for (String colName : allCols) {
            if (toDoubleCols.contains(colName)) {
                expressions[iter++] =
                        Expressions.$(colName)
                                .tryCast(DataTypes.DOUBLE())
                                .as((keepOrigin ? PREFIX : "") + colName);
            } else if (toFloatCols.contains(colName)) {
                expressions[iter++] =
                        Expressions.$(colName)
                                .tryCast(DataTypes.FLOAT())
                                .as((keepOrigin ? PREFIX : "") + colName);
            } else if (toIntCols.contains(colName)) {
                expressions[iter++] =
                        Expressions.$(colName)
                                .tryCast(DataTypes.INT())
                                .as((keepOrigin ? PREFIX : "") + colName);
            } else if (toLongCols.contains(colName)) {
                expressions[iter++] =
                        Expressions.$(colName)
                                .tryCast(DataTypes.BIGINT())
                                .as((keepOrigin ? PREFIX : "") + colName);
            } else if (toStringCols.contains(colName)) {
                expressions[iter++] =
                        Expressions.$(colName)
                                .tryCast(DataTypes.STRING())
                                .as((keepOrigin ? PREFIX : "") + colName);
            } else {
                if (!keepOrigin) {
                    expressions[iter++] = Expressions.$(colName);
                }
            }
        }

        Table middleTable = inputs[0].select(expressions);
        DataStream<Row> outputStream = tEnv.toDataStream(middleTable);
        RowTypeInfo inputTypeInfo = TableUtils.getRowTypeInfo(middleTable.getResolvedSchema());

        outputStream =
                outputStream.map(
                        (MapFunction<Row, Row>)
                                row -> {
                                    for (String colName : allCols) {
                                        if (toDoubleCols.contains(colName)) {
                                            String tmpName =
                                                    keepOrigin ? PREFIX + colName : colName;
                                            if (row.getField(tmpName) == null) {
                                                row.setField(tmpName, defaultDoubleVal);
                                            }
                                        } else if (toFloatCols.contains(colName)) {
                                            String tmpName =
                                                    keepOrigin ? PREFIX + colName : colName;
                                            if (row.getField(tmpName) == null) {
                                                row.setField(tmpName, defaultFloatVal);
                                            }
                                        } else if (toIntCols.contains(colName)) {
                                            String tmpName =
                                                    keepOrigin ? PREFIX + colName : colName;
                                            if (row.getField(tmpName) == null) {
                                                row.setField(tmpName, defaultIntVal);
                                            }
                                        } else if (toLongCols.contains(colName)) {
                                            String tmpName =
                                                    keepOrigin ? PREFIX + colName : colName;
                                            if (row.getField(tmpName) == null) {
                                                row.setField(tmpName, defaultLongVal);
                                            }
                                        } else if (toStringCols.contains(colName)) {
                                            String tmpName =
                                                    keepOrigin ? PREFIX + colName : colName;
                                            if (row.getField(tmpName) == null) {
                                                row.setField(tmpName, defaultStringVal);
                                            }
                                        }
                                    }

                                    return row;
                                },
                        inputTypeInfo);
        Table outputTable = tEnv.fromDataStream(outputStream);
        return new Table[] {outputTable};
    }

    @Override
    public void save(String path) throws IOException {
        ReadWriteUtils.saveMetadata(this, path);
    }

    public static TypeTransform load(StreamTableEnvironment env, String path) throws IOException {
        return ReadWriteUtils.loadStageParam(path);
    }

    @Override
    public Map<Param<?>, Object> getParamMap() {
        return paramMap;
    }
}
