package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import java.util.List;

public interface TestTaskDao {

  void deleteTasks(String type, String subType, TaskStatus... statuses);

  List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses);

  void deleteAllTasks();
}
