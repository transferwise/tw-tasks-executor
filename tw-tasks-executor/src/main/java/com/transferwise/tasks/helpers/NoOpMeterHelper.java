package com.transferwise.tasks.helpers;

import com.transferwise.tasks.domain.TaskStatus;

import java.util.Map;
import java.util.function.Supplier;

public class NoOpMeterHelper implements IMeterHelper {
    @Override
    public void registerTaskMarkedAsError(String bucketId, String taskType) {

    }

    @Override
    public void registerTaskProcessingStart(String bucketId, String taskType) {

    }

    @Override
    public void registerFailedTaskGrabbing(String bucketId, String taskType) {

    }

    @Override
    public void registerTaskRetryOnError(String bucketId, String taskType) {

    }

    @Override
    public void registerTaskRetry(String bucketId, String taskType) {

    }

    @Override
    public void registerTaskResuming(String bucketId, String taskType) {

    }

    @Override
    public void registerTaskMarkedAsFailed(String bucketId, String taskType) {

    }

    @Override
    public Object registerGauge(String name, Map<String, String> tags, Supplier<Number> valueSupplier) {
        return null;
    }

    @Override
    public void unregisterMetric(Object handle) {

    }

    @Override
    public void incrementCounter(String name, Map<String, String> tags, long delta) {

    }

    @Override
    public void registerTaskProcessingEnd(String bucketId, String type, long processingStartTimeMs, String processingResult) {

    }

    @Override
    public void registerKafkaCoreMessageProcessing(String topic) {

    }

    @Override
    public void registerDuplicateTask(String taskType, boolean expected) {

    }

    @Override
    public void registerScheduledTaskResuming(String taskType) {

    }

    @Override
    public void registerStuckTaskMarkedAsFailed(String taskType) {

    }

    @Override
    public void registerStuckTaskAsIgnored(String taskType) {

    }

    @Override
    public void registerStuckTaskResuming(String taskType) {

    }

    @Override
    public void registerStuckTaskMarkedAsError(String taskType) {

    }

    @Override
    public void registerStuckClientTaskResuming(String taskType) {

    }

    @Override
    public void registerFailedStatusChange(String taskType, String fromStatus, TaskStatus toStatus){

    }
}
