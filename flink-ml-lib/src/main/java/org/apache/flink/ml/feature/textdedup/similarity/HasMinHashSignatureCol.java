package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.StringParam;
import org.apache.flink.ml.param.WithParams;

/** Interface for the MinHash signature parameter. */
public interface HasMinHashSignatureCol<T> extends WithParams<T> {

    Param<String> MIN_HASH_SIGNATURE_COL =
            new StringParam("minHashSignatureCol", "MinHash signature column name", null);

    default String getMinHashSignatureCol() {
        return get(MIN_HASH_SIGNATURE_COL);
    }

    default T setMinHashSignatureCol(String value) {
        return set(MIN_HASH_SIGNATURE_COL, value);
    }
}
