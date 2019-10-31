package com.transferwise.tasks.helpers;

import java.util.Map;
import java.util.function.Supplier;

public interface IMeterHelper {
    String METRIC_PREFIX = "twTasks.";

    void registerTaskMarkedAsError(String bucketId, String taskType);

    void registerTaskProcessingStart(String bucketId, String taskType);

    void registerFailedTaskGrabbing(String bucketId, String taskType);

    void registerTaskRetryOnError(String bucketId, String taskType);

    void registerTaskRetry(String bucketId, String taskType);

    void registerTaskResuming(String bucketId, String taskType);

    void registerTaskMarkedAsFailed(String bucketId, String taskType);

    default Object registerGauge(String name, Supplier<Number> valueSupplier) {
        return registerGauge(name, null, valueSupplier);
    }

    Object registerGauge(String name, Map<String, String> tags, Supplier<Number> valueSupplier);

    void unregisterMetric(Object handle);

    default void incrementCounter(String name, long delta) {
        incrementCounter(name, null, delta);
    }

    void incrementCounter(String name, Map<String, String> tags, long delta);

    void registerTaskProcessingEnd(String bucketId, String type, long processingStartTimeMs, String processingResult);

    void registerKafkaCoreMessageProcessing(String topic);

    void registerDuplicateTask(String taskType, boolean expected);

    void registerScheduledTaskResuming(String taskType);

    void registerStuckTaskMarkedAsFailed(String taskType);

    void registerStuckTaskAsIgnored(String taskType);

    void registerStuckTaskResuming(String taskType);

    void registerStuckTaskMarkedAsError(String taskType);

    void registerStuckClientTaskResuming(String taskType);
}
