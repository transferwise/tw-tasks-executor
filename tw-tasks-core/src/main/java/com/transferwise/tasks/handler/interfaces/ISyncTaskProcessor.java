package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.ITask;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ISyncTaskProcessor extends ITaskProcessor {

  /**
   * Returning null means also DONE.
   */
  ProcessResult process(ITask task);

  @Data
  @Accessors(chain = true)
  class ProcessResult {

    ResultCode resultCode;

    public enum ResultCode {
      DONE,
      DONE_AND_DELETE,
      /**
       * For Quartz/Cron kind of jobs. Commit the current work and schedule a retry. ITaskRetryPolicy.resetTriesCount() tells if the task
       * processingTriesCount should be reset or incremented (since triesCount is how getRetryTime knows if previous processing was successful).
       */
      COMMIT_AND_RETRY
    }
  }

  default boolean isTransactional(ITask task) {
    return true;
  }
}
