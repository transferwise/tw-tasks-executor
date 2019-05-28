package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;

import java.time.ZonedDateTime;

public interface ITaskProcessingPolicy {
    /**
     * Describes what to do when task is being stuck.
     * RETRY: Resume it (creates a new task running alongside, possible duplicates)
     * MARK_AS_ERROR / MARK_AS_FAILED: Just marks task as error or failed.
     * IGNORE: Ignores the task, task will be picked up next time. Can possibly produce a task being indefinitely stuck. May be useful for testing.
     */
    enum StuckTaskResolutionStrategy {
        RETRY,
        MARK_AS_ERROR,
        MARK_AS_FAILED,
        IGNORE
    }

    default StuckTaskResolutionStrategy getStuckTaskResolutionStrategy(IBaseTask task) {
        return StuckTaskResolutionStrategy.MARK_AS_ERROR;
    }

    ZonedDateTime getMaxProcessingEndTime(IBaseTask task);

    String getProcessingBucket(IBaseTask task);

    default boolean canExecuteTaskOnThisNode(IBaseTask task) {
        return true;
    }
}
