package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

/** Interface for the ID column parameter. */
public interface HasIdCol<T> extends WithParams<T> {
    Param<String> ID_COL = new StringParam("idCol", "id column name", null);

    default String getIdCol() {
        return get(ID_COL);
    }

    default T setIdCol(String value) {
        return set(ID_COL, value);
    }
}
