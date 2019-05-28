package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;

import java.time.ZonedDateTime;

public interface ITaskRetryPolicy {
    ZonedDateTime getRetryTime(ITask task, Throwable t);

    /**
     * Should we reset the task processingTriesCount when the processing method returns COMMIT_AND_RETRY?
     * For jobs, you usually want true. For tasks that should be retried only a limited amount of time, return false.
     * This applies ONLY in case processing returning COMMIT_AND_RETRY, not on exceptions.
     */
    default boolean resetTriesCountOnSuccess(IBaseTask task) {
        return false;
    }
}
