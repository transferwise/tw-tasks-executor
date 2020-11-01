package com.transferwise.tasks.ext.management.dao;

import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_DATA;
import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_SUBTYPE;
import static com.transferwise.tasks.TaskTestBuilder.DEFAULT_TYPE;
import static com.transferwise.tasks.TaskTestBuilder.randomDoneTask;
import static com.transferwise.tasks.TaskTestBuilder.randomErrorTask;
import static com.transferwise.tasks.TaskTestBuilder.randomNewTask;
import static com.transferwise.tasks.TaskTestBuilder.randomProcessingTask;
import static com.transferwise.tasks.TaskTestBuilder.randomSubmittedTask;
import static com.transferwise.tasks.TaskTestBuilder.randomWaitingTask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.management.dao.IManagementTaskDao;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask1;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask2;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask3;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

abstract class ManagementTaskDaoIntTest extends BaseIntTest {

  @Autowired
  protected TasksProperties tasksProperties;

  @Autowired
  protected IManagementTaskDao managementTaskDao;

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
  void schedulingTaskForImmediateExecutionPutsTheTaskInWaitingState() {
    UUID taskId = randomSubmittedTask().save().getTaskId();

    boolean result = managementTaskDao.scheduleTaskForImmediateExecution(taskId, 0);

    assertTrue(result);
    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("WAITING", task.getStatus());
    assertEquals(1, task.getVersion());
  }

  @Test
  void gettingTasksInErrorStatusReturnsTasksInTheLimitOfMaxCount() {
    randomErrorTask().withType("TYPE").withSubType("1").save();
    randomErrorTask().withSubType("2").save();
    randomProcessingTask().withSubType("3").save();
    randomErrorTask().withSubType("4").save();

    List<DaoTask1> tasks = managementTaskDao.getTasksInErrorStatus(2);

    assertEquals(2, tasks.size());
    for (DaoTask1 task : tasks) {
      assertFalse(task.getSubType().contains("3"));
    }
  }

  @Test
  void gettingTasksInProcessingOrWaitingStatusReturnsTasksInTheLimitOfMaxCount() {
    randomProcessingTask().withSubType("1").save();
    randomErrorTask().withSubType("2").save();
    randomWaitingTask().withSubType("3").save();
    randomProcessingTask().withSubType("4").save();
    randomWaitingTask().withSubType("5").save();

    List<DaoTask3> tasks = managementTaskDao.getTasksInProcessingOrWaitingStatus(3);

    assertEquals(3, tasks.size());
    assertEquals(3, tasks.stream().filter(t -> ImmutableSet.of("1", "3", "4", "5").contains(t.getSubType())).count());
    assertEquals(
        ImmutableSet.of(TaskStatus.PROCESSING.name(), TaskStatus.WAITING.name()),
        tasks.stream()
            .map(DaoTask3::getStatus)
            .collect(Collectors.toSet())
    );
  }

  @Test
  void gettingStuckTasksReturnsTasksInTheLimitOfMaxCount() {
    randomProcessingTask().save();
    randomSubmittedTask().save();
    randomNewTask().save();
    randomWaitingTask().save();
    randomDoneTask().save();
    randomWaitingTask().save();

    List<DaoTask2> tasks = managementTaskDao.getStuckTasks(4, Duration.ofMillis(-2));

    assertEquals(4, tasks.size());
  }

  @Test
  void gettingTasksByUuidListWorks() {
    final UUID taskId1 = randomDoneTask().save().getTaskId();
    randomDoneTask().save();
    final UUID taskId2 = randomDoneTask().save().getTaskId();
    randomDoneTask().save();
    final UUID taskId3 = randomDoneTask().save().getTaskId();
    final UUID taskId4 = randomDoneTask().save().getTaskId();
    final UUID taskId5 = randomDoneTask().save().getTaskId();
    final UUID taskId6 = randomDoneTask().save().getTaskId();
    final UUID taskId7 = randomDoneTask().save().getTaskId();

    List<FullTaskRecord> tasks = managementTaskDao.getTasks(Arrays.asList(taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7));

    assertEquals(7, tasks.size());
    assertEquals(
        ImmutableSet.of(taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7),
        tasks.stream().map(FullTaskRecord::getId).collect(Collectors.toSet())
    );

    // task is correctly populated
    FullTaskRecord task1 = tasks.stream().filter(t -> t.getId().equals(taskId1)).findAny()
        .orElseThrow(() -> new NullPointerException("No task found."));
    assertEquals("DONE", task1.getStatus());
    assertEquals(DEFAULT_TYPE, task1.getType());
    assertEquals(5, task1.getPriority());
    assertEquals(0, task1.getVersion());
    assertEquals(DEFAULT_DATA, task1.getData());
    assertEquals(DEFAULT_SUBTYPE, task1.getSubType());
    assertEquals(0, task1.getProcessingTriesCount());
    assertNull(task1.getProcessingClientId());
    assertNotNull(task1.getStateTime());
    assertNotNull(task1.getNextEventTime());
  }
}
