package org.apache.flink.ml.feature.textdedup.similarity;

import org.apache.flink.ml.param.IntParam;
import org.apache.flink.ml.param.Param;
import org.apache.flink.ml.param.ParamValidators;
import org.apache.flink.ml.param.WithParams;

/** Interface for the bucket chunk size parameter. */
public interface HasBucketChunkSize<T> extends WithParams<T> {
    Param<Integer> BUCKET_CHUNK_SIZE =
            new IntParam(
                    "bucketChunkSize",
                    "Divides items in a bucket to multiple chunks for faster calculation.",
                    65536,
                    ParamValidators.gt(1));

    default int getBucketChunkSize() {
        return get(BUCKET_CHUNK_SIZE);
    }

    default T setBucketChunkSize(int value) {
        return set(BUCKET_CHUNK_SIZE, value);
    }
}
