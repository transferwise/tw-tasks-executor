package com.transferwise.tasks;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDao.InsertTaskRequest;
import com.transferwise.tasks.dao.ITaskDao.InsertTaskResponse;
import com.transferwise.tasks.domain.TaskStatus;
import java.time.ZonedDateTime;
import java.util.UUID;

public class TaskTestBuilder {

  public static final String DEFAULT_SUBTYPE = "SUBTYPE";
  public static final String DEFAULT_TYPE = "TEST";
  public static final String DEFAULT_DATA = "DATA";
  public static final int DEFAULT_PRIORITY = 5;

  private ITaskDao.InsertTaskRequest insertTaskRequest;

  public static TaskTestBuilder randomWaitingTask() {
    return randomTask().withStatus(TaskStatus.WAITING);
  }

  public static TaskTestBuilder randomErrorTask() {
    return randomTask().withStatus(TaskStatus.ERROR);
  }

  public static TaskTestBuilder randomNewTask() {
    return randomTask().withStatus(TaskStatus.NEW);
  }

  public static TaskTestBuilder randomProcessingTask() {
    return randomTask().withStatus(TaskStatus.PROCESSING);
  }

  public static TaskTestBuilder randomSubmittedTask() {
    return randomTask().withStatus(TaskStatus.SUBMITTED);
  }

  public static TaskTestBuilder randomDoneTask() {
    return randomTask().withStatus(TaskStatus.DONE);
  }

  public static TaskTestBuilder randomTask() {
    TaskTestBuilder builder = new TaskTestBuilder();
    builder.insertTaskRequest = new InsertTaskRequest();
    return builder.withType(DEFAULT_TYPE)
        .withSubType(DEFAULT_SUBTYPE)
        .withData(DEFAULT_DATA)
        .withMaxStuckTime(ZonedDateTime.now(TwContextClockHolder.getClock()))
        .withId(UuidUtils.generatePrefixCombUuid())
        .withPriority(DEFAULT_PRIORITY);
  }

  public static TaskTestBuilder newTask() {
    TaskTestBuilder b = new TaskTestBuilder();
    b.insertTaskRequest = new ITaskDao.InsertTaskRequest();
    return b.withType("test").withData("Hello World!").withPriority(DEFAULT_PRIORITY);
  }

  public TaskTestBuilder withPriority(int priority) {
    insertTaskRequest.setPriority(priority);
    return this;
  }

  public TaskTestBuilder withData(String data) {
    insertTaskRequest.setData(data);
    return this;
  }

  public TaskTestBuilder withType(String type) {
    insertTaskRequest.setType(type);
    return this;
  }

  public TaskTestBuilder withSubType(String subType) {
    insertTaskRequest.setSubType(subType);
    return this;
  }

  public TaskTestBuilder withId(UUID id) {
    insertTaskRequest.setTaskId(id);
    return this;
  }

  public TaskTestBuilder inStatus(TaskStatus status) {
    insertTaskRequest.setStatus(status);
    return this;
  }

  public TaskTestBuilder withMaxStuckTime(ZonedDateTime time) {
    insertTaskRequest.setMaxStuckTime(time);
    return this;
  }

  public TaskTestBuilder withStatus(TaskStatus status) {
    insertTaskRequest.setStatus(status);
    return this;
  }

  public TaskTestBuilder withKey(String key) {
    insertTaskRequest.setKey(key);
    return this;
  }

  public InsertTaskResponse save() {
    return TestApplicationContextHolder.getApplicationContext().getBean(ITaskDao.class).insertTask(insertTaskRequest);
  }
}
