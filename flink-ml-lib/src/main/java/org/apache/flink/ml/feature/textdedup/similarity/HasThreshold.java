package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.DoubleParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.WithParams;

/** Interface for the distance threshold parameter. */
public interface HasThreshold<T> extends WithParams<T> {
    Param<Double> THRESHOLD =
            new DoubleParam("threshold", "Distance threshold.", 0.3, ParamValidators.gt(0.0));

    default Double getThreshold() {
        return get(THRESHOLD);
    }

    default T setThreshold(Double value) {
        return set(THRESHOLD, value);
    }
}
