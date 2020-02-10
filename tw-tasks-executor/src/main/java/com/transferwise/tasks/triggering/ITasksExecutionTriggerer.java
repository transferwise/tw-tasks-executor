package com.transferwise.tasks.triggering;

import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.domain.BaseTask;
import java.util.concurrent.Future;

public interface ITasksExecutionTriggerer {

  void trigger(BaseTask task);

  void startTasksProcessing(String bucketId);

  Future<Void> stopTasksProcessing(String bucketId);

  ITasksService.TasksProcessingState getTasksProcessingState(String bucketId);
}
