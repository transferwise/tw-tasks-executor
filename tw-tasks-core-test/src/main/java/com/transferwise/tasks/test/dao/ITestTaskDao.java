package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.dao.ITaskDaoDataSerializer.SerializedData;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import java.util.List;
import java.util.UUID;

public interface ITestTaskDao {

  void deleteTasks(String type, String subType, TaskStatus... statuses);

  List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses);

  void deleteAllTasks();

  SerializedData getSerializedData(UUID taskId);
}
