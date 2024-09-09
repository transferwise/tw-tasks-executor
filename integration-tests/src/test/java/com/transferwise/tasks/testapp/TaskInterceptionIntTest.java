package com.transferwise.tasks.testapp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.TasksService;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskInterceptionIntTest extends BaseIntTest {

  @Autowired
  private TasksService tasksService;

  @Test
  void jambiTaskIsInterceptedCorrectly() {
    testTasksService.resetAndDeleteTasksWithTypes("test");
    tasksService.addTask(new AddTaskRequest().setTaskId(UuidUtils.generatePrefixCombUuid()).setType("test").setSubType("Jambi"));
    tasksService.addTask(new AddTaskRequest().setTaskId(UuidUtils.generatePrefixCombUuid()).setType("test").setSubType("Jambi"));
    Awaitility.await().until(() -> testTasksService.getFinishedTasks("test", "Jambi").size() == 2);
    assertEquals(2, meterRegistry.counter("tool", "song", "eulogy").count());

  }

}
