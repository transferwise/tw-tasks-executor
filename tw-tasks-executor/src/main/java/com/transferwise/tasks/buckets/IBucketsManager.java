package com.transferwise.tasks.buckets;

import java.util.List;

public interface IBucketsManager {
    String DEFAULT_ID = "default";

    BucketProperties getBucketProperties(String bucketId);

    void registerBucketProperties(String bucketId, BucketProperties bucketProperties);

    List<String> getBucketIds();

    boolean isConfiguredBucket(String bucketId);
}
