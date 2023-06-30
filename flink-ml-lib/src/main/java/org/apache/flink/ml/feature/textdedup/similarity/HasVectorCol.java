package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

/** Interface for the vector column name parameter. */
public interface HasVectorCol<T> extends WithParams<T> {

    Param<String> VECTOR_COL = new StringParam("vectorCol", "Name of a vector column", null);

    default String getVectorCol() {
        return get(VECTOR_COL);
    }

    default T setVectorCol(String colName) {
        return set(VECTOR_COL, colName);
    }
}
