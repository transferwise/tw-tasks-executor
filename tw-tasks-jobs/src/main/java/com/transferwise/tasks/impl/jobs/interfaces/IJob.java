package com.transferwise.tasks.impl.jobs.interfaces;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Beans implementing this interface will be picked up by the engine and executed accordingly, as quite normal tw-tasks.
 */
public interface IJob {

  ZonedDateTime getNextRunTime();

  /**
   * Useful for cases, when Job is set up to be executed once per day and we would want to have retries during that.
   *
   * <p>This interface's `getRetryTime` is consulted when a processing results in ERROR.
   * If it returns null, we will resume with normal executions plan, otherwise we will do an intermediate retry.
   */
  default ITaskRetryPolicy getFailureRetryPolicy() {
    return null;
  }

  default String getUniqueName() {
    if (getClass().isAnonymousClass()) {
      throw new UnsupportedOperationException("We can not figure out a good name for anonymous class. Please override this method.");
    }
    return getClass().getSimpleName();
  }

  default boolean isTransactional() {
    return true;
  }

  default ITaskConcurrencyPolicy getConcurrencyPolicy() {
    return null;
  }

  default ITaskProcessingPolicy getProcessingPolicy() {
    return null;
  }

  default UUID getTaskId() {
    return UUID.nameUUIDFromBytes(getUniqueName().getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Saved as task's data field when task is initially registered.
   */
  default String getInitialData() {
    return null;
  }

  /**
   * Added to the task's type.
   */
  default String getTaskTypeSuffix() {
    return null;
  }

  /**
   * Returning null is the same as returning `ProcessResult` with resultCode of SUCCESS.
   */
  ProcessResult process(ITask task);

  @Data
  @Accessors(chain = true)
  class ProcessResult {

    ResultCode resultCode;

    public enum ResultCode {
      SUCCESS, // Job will executed again at getNextRunTime().
      END // Job will never executed again.
    }
  }
}
