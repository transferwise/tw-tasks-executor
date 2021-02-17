package com.transferwise.tasks.handler.interfaces;

import com.transferwise.common.context.Criticality;
import com.transferwise.tasks.domain.IBaseTask;
import java.time.Duration;
import java.time.Instant;
import lombok.NonNull;

@SuppressWarnings({"SameReturnValue", "unused"})
public interface ITaskProcessingPolicy {

  /**
   * Describes what to do when task is being stuck. RETRY: Resume it (creates a new task running alongside, possible duplicates) MARK_AS_ERROR /
   * MARK_AS_FAILED: Just marks task as error or failed. IGNORE: Ignores the task, task will be picked up next time. Can possibly produce a task being
   * indefinitely stuck. May be useful for testing.
   */
  enum StuckTaskResolutionStrategy {
    RETRY,
    MARK_AS_ERROR,
    MARK_AS_FAILED,
    IGNORE
  }

  enum StuckDetector {
    /**
     * We found `PROCESSING` tasks with the same `processing_client_id` as our `node-id` configuration.
     */
    SAME_NODE_STARTUP,
    /**
     * The normal cluster wide stuck tasks detector.
     */
    CLUSTER_WIDE_STUCK_TASKS_DETECTOR
  }

  default StuckTaskResolutionStrategy getStuckTaskResolutionStrategy(IBaseTask task, StuckDetector stuckDetector) {
    return StuckTaskResolutionStrategy.MARK_AS_ERROR;
  }

  @NonNull
  Instant getProcessingDeadline(IBaseTask task);

  @NonNull
  default Criticality getProcessingCriticality(IBaseTask task) {
    return Criticality.SHEDDABLE_PLUS;
  }

  /**
   * Usually a team name owning the task/code.
   */
  default String getOwner(IBaseTask task) {
    return null;
  }

  default Duration getExpectedQueueTime(IBaseTask task) {
    return null;
  }

  String getProcessingBucket(IBaseTask task);

  default boolean canExecuteTaskOnThisNode(IBaseTask task) {
    return true;
  }
}
