package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

/** Interface for the incremental indicator column parameter. */
public interface HasIncrementalIndicatorCol<T> extends WithParams<T> {
    Param<String> INCREMENTAL_INDICATOR_COL =
            new StringParam("incrementalIndicatorCol", "Incremental indicator column name", null);

    default String getIncrementalIndicatorCol() {
        return get(INCREMENTAL_INDICATOR_COL);
    }

    default T setIncrementalIndicatorCol(String value) {
        return set(INCREMENTAL_INDICATOR_COL, value);
    }
}
