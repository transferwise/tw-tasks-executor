package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;

public interface ITaskConcurrencyPolicy {

  boolean bookSpaceForTask(IBaseTask task);

  void freeSpaceForTask(IBaseTask task);
}
