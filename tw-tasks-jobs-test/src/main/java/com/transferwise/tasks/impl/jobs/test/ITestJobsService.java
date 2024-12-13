package com.transferwise.tasks.impl.jobs.test;

import com.transferwise.tasks.domain.ITaskVersionId;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJobsService;
import java.util.Collection;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITestJobsService extends IJobsService {

  ExecuteAsyncHandle executeAsync(IJob job);

  boolean hasFinished(ExecuteAsyncHandle handle);

  /**
   * Clear all existing jobs and related tw-tasks, and initialize them again (if JobsProperties.autoInitialize is true).
   */
  void reset();

  /**
   * Clear all existing jobs and related tw-tasks and initialize with the provided jobs.
   * Most likely it would be useful when `JobsProperties.autoInitialize` is false,
   * when only specific jobs are needed to be initialized for the test, so that other jobs won't run.
   *
   * @param jobs jobs to initialize (register and create related tw-tasks)
   */
  void resetAndInitialize(Collection<IJob> jobs);

  @Data
  @Accessors(chain = true)
  class ExecuteAsyncHandle {

    private ITaskVersionId taskVersionId;
  }
}
