package com.transferwise.tasks.test;

import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.TaskStatus;
import java.time.ZonedDateTime;
import java.util.UUID;

public class TaskTestBuilder extends BaseTestBuilder<TaskTestBuilder, UUID> {

  private static final int DEFAULT_PRIORITY = 5;

  private ITaskDao.InsertTaskRequest insertTaskRequest;

  public static TaskTestBuilder newTask() {
    TaskTestBuilder b = new TaskTestBuilder();
    b.insertTaskRequest = new ITaskDao.InsertTaskRequest();
    return b.withType("test").withData("Hello World!").withPriority(DEFAULT_PRIORITY);
  }

  public TaskTestBuilder withPriority(int priority) {
    insertTaskRequest.setPriority(priority);
    return me();
  }

  public TaskTestBuilder withData(String data) {
    insertTaskRequest.setData(data);
    return me();
  }

  public TaskTestBuilder withType(String type) {
    insertTaskRequest.setType(type);
    return me();
  }

  public TaskTestBuilder withSubType(String subType) {
    insertTaskRequest.setSubType(subType);
    return me();
  }

  public TaskTestBuilder withId(UUID id) {
    insertTaskRequest.setTaskId(id);
    return me();
  }

  public TaskTestBuilder inStatus(TaskStatus status) {
    insertTaskRequest.setStatus(status);
    return me();
  }

  public TaskTestBuilder withMaxStuckTime(ZonedDateTime time) {
    insertTaskRequest.setMaxStuckTime(time);
    return me();
  }

  @Override
  public UUID build() {
    return TestApplicationContextHolder.getApplicationContext().getBean(ITaskDao.class).insertTask(insertTaskRequest).getTaskId();
  }
}
