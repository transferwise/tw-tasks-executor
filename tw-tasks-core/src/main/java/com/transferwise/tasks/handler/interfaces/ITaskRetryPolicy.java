package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import java.time.ZonedDateTime;

@SuppressWarnings("unused")
public interface ITaskRetryPolicy {

  ZonedDateTime getRetryTime(ITask task, Throwable t);

  /**
   * Should we reset the task processingTriesCount when the processing method returns COMMIT_AND_RETRY? For jobs, you usually want true. For tasks
   * that should be retried only a limited amount of time, return false. This applies ONLY in case processing returning COMMIT_AND_RETRY, not on
   * exceptions.
   */
  default boolean resetTriesCountOnSuccess(IBaseTask task) {
    return false;
  }

  /**
   * By default, when a task fails with a processing error, we log it with ERROR level and retry it according to the retry policy. Override this
   * method with custom exception handler to change the default behavior. For example, if you want to log it differently or log it only on the last
   * retry.
   *
   * @return the custom exception handler or {@code null} if the default behavior should be used
   */
  default ITaskExceptionHandler getExceptionHandler() {
    return null;
  }
}
