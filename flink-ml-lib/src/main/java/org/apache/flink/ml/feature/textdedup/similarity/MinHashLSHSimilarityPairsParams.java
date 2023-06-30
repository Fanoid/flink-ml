package org.apache.flink.ml.feature.textdedup.similarity;

/**
 * Params for {@link MinHashLSHSimilarityPairs}.
 *
 * @param <T>
 */
public interface MinHashLSHSimilarityPairsParams<T>
        extends HasIdCol<T>,
                HasVectorCol<T>,
                HasMinHashSignatureCol<T>,
                HasThreshold<T>,
                HasBucketChunkSize<T>,
                HasItemCandidateLimitInBucket<T>,
                HasIncrementalIndicatorCol<T> {}
