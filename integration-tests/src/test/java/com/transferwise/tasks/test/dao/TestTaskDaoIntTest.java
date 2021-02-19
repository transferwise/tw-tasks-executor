package com.transferwise.tasks.test.dao;

import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_DATA;
import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_SUBTYPE;
import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_TYPE;
import static com.transferwise.tasks.TaskTestBuilder.randomDoneTask;
import static com.transferwise.tasks.TaskTestBuilder.randomProcessingTask;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

abstract class TestTaskDaoIntTest extends BaseIntTest {

  @Autowired
  protected TasksProperties tasksProperties;

  @Autowired
  protected ITestTaskDao testTaskDao;

  @Autowired
  protected ITaskDao taskDao;

  @BeforeEach
  void taskDaoIntTestSetup() {
    testTasksService.stopProcessing();
  }

  @AfterEach
  void taskDaoIntTestCleanup() {
    tasksProperties.setParanoidTasksCleaning(false);
  }

  @Test
  void findingTasksByTypeReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = randomProcessingTask().withType(type).save().getTaskId();

    List<Task> tasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, null);

    assertEquals(1, tasks.size());

    Task task = tasks.get(0);
    assertEquals(taskId, task.getId());
    assertEquals(type, task.getType());
    assertEquals(DEFAULT_SUBTYPE, task.getSubType());
    assertThat(task.getData()).isEqualTo(DEFAULT_DATA);
    assertEquals("PROCESSING", task.getStatus());
    assertEquals(0, task.getVersion());
    assertEquals(0, task.getProcessingTriesCount());
    assertEquals(5, task.getPriority());
  }

  @Test
  void findingTasksByTypeAndSubtypeReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    String subType = "MY_SUBTYPE";
    UUID taskId = randomProcessingTask().withType(type).withSubType(subType).save().getTaskId();
    randomDoneTask().save();

    List<Task> tasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, subType);

    assertEquals(1, tasks.size());
    assertEquals(taskId, tasks.get(0).getId());
    assertEquals(subType, tasks.get(0).getSubType());
  }

  @Test
  void findingTasksByTypeAndStatusReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = randomProcessingTask().withType(type).save().getTaskId();
    randomDoneTask().save();

    List<Task> tasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING);

    assertEquals(1, tasks.size());
    assertEquals(taskId, tasks.get(0).getId());
    assertEquals("PROCESSING", tasks.get(0).getStatus());
  }

  @Test
  void deletingAllTasksShouldDeleteAllTasks() {
    UUID taskId = randomProcessingTask().save().getTaskId();

    testTaskDao.deleteAllTasks();

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertNull(fullTaskRecord);
  }

  @Test
  void deletingTasksByTypeDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    randomProcessingTask().withType(type).save();
    randomDoneTask().save();

    testTaskDao.deleteTasks(type, null);

    List<Task> deletedTasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, null);
    assertEquals(0, deletedTasks.size());

    List<Task> remainingTasks = testTaskDao.findTasksByTypeSubTypeAndStatus("TEST", null);
    assertEquals(1, remainingTasks.size());
  }

  @Test
  void deletingTasksByTypeAndSubtypeDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    String subType = "MY_SUBTYPE";
    randomProcessingTask().withType(type).withSubType(subType).save();
    randomProcessingTask().withType("TEST").withSubType("SUBTYPE").save();

    testTaskDao.deleteTasks(type, subType);

    List<Task> deletedTasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, subType);
    assertEquals(0, deletedTasks.size());
    List<Task> remainingTasks = testTaskDao.findTasksByTypeSubTypeAndStatus("TEST", "SUBTYPE");
    assertEquals(1, remainingTasks.size());
  }

  @Test
  void deletingTasksByTypeAndStatusDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    randomProcessingTask().withType(type).save();
    randomDoneTask().save();

    testTaskDao.deleteTasks(type, null, TaskStatus.PROCESSING);

    List<Task> deletedTasks = testTaskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING);
    assertEquals(0, deletedTasks.size());
    List<Task> remainingTasks = testTaskDao.findTasksByTypeSubTypeAndStatus(DEFAULT_TYPE, null, TaskStatus.DONE);
    assertEquals(1, remainingTasks.size());
  }
}
