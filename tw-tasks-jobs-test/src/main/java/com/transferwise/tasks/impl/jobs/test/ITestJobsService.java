package com.transferwise.tasks.impl.jobs.test;

import com.transferwise.tasks.domain.ITaskVersionId;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJobsService;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITestJobsService extends IJobsService {

  ExecuteAsyncHandle executeAsync(IJob job);

  boolean hasFinished(ExecuteAsyncHandle handle);

  void reset();

  @Data
  @Accessors(chain = true)
  class ExecuteAsyncHandle {

    private ITaskVersionId taskVersionId;
  }
}
