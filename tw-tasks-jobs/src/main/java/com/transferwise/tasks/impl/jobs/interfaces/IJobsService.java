package com.transferwise.tasks.impl.jobs.interfaces;

import com.transferwise.tasks.domain.IBaseTask;

public interface IJobsService {

  IJob getJobFor(IBaseTask task);

  void register(IJob job);
}
