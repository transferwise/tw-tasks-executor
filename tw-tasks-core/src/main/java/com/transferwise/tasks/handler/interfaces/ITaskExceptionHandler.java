package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.ITask;
import java.time.ZonedDateTime;

@FunctionalInterface
public interface ITaskExceptionHandler {

  /**
   * Handle the exception thrown during task processing.
   *
   * @param task      task
   * @param t         thrown exception
   * @param retryTime next retry time, {@code null} if no next retry
   */
  void handle(Throwable t, ITask task, ZonedDateTime retryTime);
}