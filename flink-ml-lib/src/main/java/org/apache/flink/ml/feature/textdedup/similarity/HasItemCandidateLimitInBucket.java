package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.WithParams;

/** Interface for the item candidate limit in buckets parameter. */
public interface HasItemCandidateLimitInBucket<T> extends WithParams<T> {

    Param<Integer> ITEM_CANDIDATE_LIMIT_IN_BUCKET =
            new IntParam(
                    "itemCandidateLimitInBucket",
                    "The limit of the number of pairs to be checked for each item in a bucket",
                    Integer.MAX_VALUE,
                    ParamValidators.gt(1));

    default int getItemCandidateLimitInBucket() {
        return get(ITEM_CANDIDATE_LIMIT_IN_BUCKET);
    }

    default T setItemCandidateLimitInBucket(int value) {
        return set(ITEM_CANDIDATE_LIMIT_IN_BUCKET, value);
    }
}
