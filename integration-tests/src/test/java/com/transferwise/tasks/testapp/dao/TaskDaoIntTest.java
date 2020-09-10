package com.transferwise.tasks.testapp.dao;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDao.DaoTask1;
import com.transferwise.tasks.dao.ITaskDao.DeleteFinishedOldTasksResult;
import com.transferwise.tasks.dao.ITaskDao.GetStuckTasksResponse;
import com.transferwise.tasks.dao.ITaskDao.InsertTaskResponse;
import com.transferwise.tasks.dao.ITaskDao.StuckTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

abstract class TaskDaoIntTest extends BaseIntTest {

  @Autowired
  private ITaskDao taskDao;

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Autowired
  private TasksProperties tasksProperties;

  @BeforeEach
  void taskDaoIntTestSetup() {
    testTasksService.stopProcessing();
  }

  @AfterEach
  void taskDaoIntTestCleanup() {
    tasksProperties.setParanoidTasksCleaning(false);
  }

  @Test
  void insertingATaskInsertsOnlyOneTaskForAGivenId() {
    UUID taskId = UUID.randomUUID();

    InsertTaskResponse result1 = addTask(taskId, TaskStatus.SUBMITTED);
    InsertTaskResponse result2 = addTask(taskId, TaskStatus.DONE);

    assertEquals(taskId, result1.getTaskId());
    assertTrue(result1.isInserted());
    assertNull(result2.getTaskId());
    assertFalse(result2.isInserted());

    assertEquals(taskId, taskDao.getTask(taskId, BaseTask1.class).getId());
    assertEquals(1, taskDao.getTasksCountInStatus(10, TaskStatus.SUBMITTED));
  }

  @Test
  void insertingATaskWithUniqueKeyTwiceCreatesOnlyOneTask() {
    String key = "Hello World";

    // TODO: Replace with a test-builder.
    InsertTaskResponse response = taskDao.insertTask(new ITaskDao.InsertTaskRequest()
        .setStatus(TaskStatus.NEW)
        .setKey(key)
        .setData("")
        .setPriority(5)
        .setMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
        .setType("Test"));

    assertNotNull(response.getTaskId());

    response = taskDao.insertTask(new ITaskDao.InsertTaskRequest()
        .setStatus(TaskStatus.NEW)
        .setKey(key)
        .setData("")
        .setMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
        .setType("Test")
        .setPriority(5));

    assertFalse(response.isInserted());
    assertNull(response.getTaskId());
  }

  @Test
  void gettingATaskReturnsTheCorrectTaskForBaseTask1() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId);
    addTask();

    BaseTask1 task = taskDao.getTask(taskId, BaseTask1.class);

    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertEquals("TEST", task.getType());
    assertEquals(5, task.getPriority());
    assertEquals(0, task.getVersion());
  }

  @Test
  void gettingATaskReturnsTehCorrectTaskForFullTaskRecord() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId);
    addTask();

    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);

    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertEquals("TEST", task.getType());
    assertEquals(5, task.getPriority());
    assertEquals(0, task.getVersion());
    assertEquals("DATA", task.getData());
    assertEquals("SUBTYPE", task.getSubType());
    assertEquals(0, task.getProcessingTriesCount());
    assertNull(task.getProcessingClientId());
    assertNotNull(task.getStateTime());
    assertNotNull(task.getNextEventTime());
  }

  @Test
  void settingToBeRetriedSetsTheCorrectRetryTime() {
    UUID taskId = UUID.randomUUID();
    ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2);
    addTask(taskId);

    boolean result = taskDao.setToBeRetried(taskId, retryTime, 0, false);

    assertTrue(result);

    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("WAITING", task.getStatus());
    assertEquals(1, task.getVersion());
    assertEquals(0, task.getProcessingTriesCount());
    assertNotNull(task.getStateTime());
    assertEquals(retryTime.toInstant(), task.getNextEventTime().toInstant());
  }

  @Test
  void grabbingForProcessingIncrementsTheProcessingTriesCount() {
    UUID taskId = UUID.randomUUID();
    String nodeId = "testNode";
    Instant processingDeadline = Instant.now().plus(Duration.ofHours(2));
    addTask(taskId, TaskStatus.SUBMITTED);
    Task task = taskDao.getTask(taskId, Task.class);

    Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    assertEquals(taskId, returnedTask.getId());
    assertEquals("PROCESSING", returnedTask.getStatus());
    assertEquals(1, returnedTask.getVersion());
    assertEquals(1, returnedTask.getProcessingTriesCount());

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(processingDeadline, fullTaskRecord.getNextEventTime().toInstant());
    assertEquals(nodeId, fullTaskRecord.getProcessingClientId());
  }

  @Test
  void settingToBeRetriedResetTriesCountIfSpecified() {
    UUID taskId = UUID.randomUUID();
    String nodeId = "testNode";
    ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2);
    addTask(taskId, TaskStatus.SUBMITTED);
    Task task = taskDao.getTask(taskId, Task.class);
    taskDao.grabForProcessing(task.toBaseTask(), nodeId, ZonedDateTime.now().plusHours(1).toInstant());

    boolean result = taskDao.setToBeRetried(taskId, retryTime, 1, true);

    assertTrue(result);
    FullTaskRecord finalTask = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, finalTask.getId());
    assertEquals("WAITING", finalTask.getStatus());
    assertEquals(2, finalTask.getVersion());
    assertEquals(0, finalTask.getProcessingTriesCount());
    assertNotNull(finalTask.getStateTime());
    assertEquals(retryTime.toInstant(), finalTask.getNextEventTime().toInstant());
  }

  @Test
  void grabbingForProcessingReturnsNullIfNoTaskMatchesTheOnePassed() {
    UUID taskId = UUID.randomUUID();
    String nodeId = "testNode";
    Instant processingDeadline = Instant.now().plus(Duration.ofHours(2));
    addTask(taskId, TaskStatus.SUBMITTED);
    Task task = new Task().setId(UUID.randomUUID());

    Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    assertNull(returnedTask);

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(0, fullTaskRecord.getVersion());
    assertEquals(0, fullTaskRecord.getProcessingTriesCount());
  }

  @Test
  void settingStatusUpdatesTheStatusCorrectly() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.SUBMITTED);

    boolean result = taskDao.setStatus(taskId, TaskStatus.WAITING, 0);

    assertTrue(result);
    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("WAITING", task.getStatus());
    assertEquals(1, task.getVersion());
  }

  //TODO: Flaky test. WAITING task can be picked up and moved to ERROR, before we assert.
  @Test
  void schedulingTaskForImmediateExecutionPutsTheTaskInWaitingState() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.SUBMITTED);

    boolean result = taskDao.scheduleTaskForImmediateExecution(taskId, 0);

    assertTrue(result);
    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("WAITING", task.getStatus());
    assertEquals(1, task.getVersion());
  }

  @Test
  void getStuckTasksReturnsAllTasksToRetry() {
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);
    final UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.SUBMITTED);
    final ZonedDateTime oldNextEventTime = taskDao.getTask(taskId, FullTaskRecord.class).getNextEventTime();
    addRandomTask(TaskStatus.SUBMITTED);
    testClock.tick(Duration.ofMillis(1));

    GetStuckTasksResponse result = taskDao.getStuckTasks(2, TaskStatus.SUBMITTED);

    assertFalse(result.isHasMore());
    assertEquals(2, result.getStuckTasks().size());
    assertEquals(0, result.getStuckTasks().get(0).getVersionId().getVersion());
    assertEquals("SUBMITTED", result.getStuckTasks().get(0).getStatus());
    assertEquals(0, result.getStuckTasks().get(1).getVersionId().getVersion());
    assertEquals("SUBMITTED", result.getStuckTasks().get(1).getStatus());

    // task has not changed
    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(oldNextEventTime, fullTaskRecord.getNextEventTime());
  }

  @Test
  void markAsSubmittedAndSetNextEventTimePutsTheTaskInSubmittedStateAndUpdatesNextEventTime() {
    ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2);
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.NEW);
    addRandomTask(TaskStatus.NEW);

    boolean result = taskDao.markAsSubmitted(taskId, 0, maxStuckTime);

    assertTrue(result);
    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(1, fullTaskRecord.getVersion());
    assertEquals(maxStuckTime.toInstant(), fullTaskRecord.getNextEventTime().toInstant());
  }

  @Test
  void preparingStuckOnProcessingTasksForResumingPutsTheTaskInSubmittedState() {
    Instant processingDeadline = ZonedDateTime.now().plusHours(2).toInstant();
    UUID taskId = UUID.randomUUID();
    String nodeId = "testNode";
    addTask(taskId, TaskStatus.SUBMITTED);
    Task task = taskDao.getTask(taskId, Task.class);
    taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    List<StuckTask> tasks = taskDao.prepareStuckOnProcessingTasksForResuming(
        nodeId, ZonedDateTime.ofInstant(processingDeadline, ZoneId.systemDefault())
    );

    assertEquals(1, tasks.size());
    assertEquals(2, tasks.get(0).getVersionId().getVersion());

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(processingDeadline, fullTaskRecord.getNextEventTime().toInstant());
  }

  @Test
  void markingAsSubmittedUpdatesTheStateCorrectly() {
    ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2);
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING);

    boolean result = taskDao.markAsSubmitted(taskId, 0, maxStuckTime);

    assertTrue(result);
    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(maxStuckTime.toInstant(), fullTaskRecord.getNextEventTime().toInstant());
  }

  @Test
  void findingTasksByTypeReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type);
    addRandomTask();

    List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null);

    assertEquals(1, tasks.size());

    Task task = tasks.get(0);
    assertEquals(taskId, task.getId());
    assertEquals(type, task.getType());
    assertEquals("SUBTYPE", task.getSubType());
    assertEquals("DATA", task.getData());
    assertEquals("PROCESSING", task.getStatus());
    assertEquals(0, task.getVersion());
    assertEquals(0, task.getProcessingTriesCount());
    assertEquals(5, task.getPriority());
  }

  @Test
  void findingTasksByTypeAndSubtypeReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    String subType = "MY_SUBTYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type, subType);
    addRandomTask();

    List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, subType);

    assertEquals(1, tasks.size());
    assertEquals(taskId, tasks.get(0).getId());
    assertEquals(subType, tasks.get(0).getSubType());
  }

  @Test
  void findingTasksByTypeAndStatusReturnsTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type);
    addRandomTask();

    List<Task> tasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING);

    assertEquals(1, tasks.size());
    assertEquals(taskId, tasks.get(0).getId());
    assertEquals("PROCESSING", tasks.get(0).getStatus());
  }

  @Test
  void gettingTaskCountInStatusReturnsTheCorrectValue() {
    addRandomTask(TaskStatus.PROCESSING);
    addRandomTask(TaskStatus.PROCESSING);
    addRandomTask();

    int count = taskDao.getTasksCountInStatus(10, TaskStatus.PROCESSING);

    assertEquals(2, count);
  }

  @Test
  void gettingErrorTasksInGroup() {
    addRandomTask();
    addRandomTask(TaskStatus.ERROR);
    addRandomTask(TaskStatus.ERROR);
    addRandomTask(TaskStatus.ERROR, 1, "XXX");

    List<Pair<String, Integer>> tasksInErrorStatus = taskDao.getTasksCountInErrorGrouped(10);

    assertEquals(2, tasksInErrorStatus.size());
    assertEquals("TEST", tasksInErrorStatus.get(0).getLeft());
    assertEquals(2, tasksInErrorStatus.get(0).getRight());
    assertEquals("XXX", tasksInErrorStatus.get(1).getLeft());
    assertEquals(1, tasksInErrorStatus.get(1).getRight());
  }

  @Test
  void gettingStuckTaskCountConsidersTheCorrectStatus() {
    addRandomTask(TaskStatus.PROCESSING);
    addRandomTask(TaskStatus.SUBMITTED);
    addRandomTask(TaskStatus.NEW);
    addRandomTask(TaskStatus.WAITING);
    addRandomTask(TaskStatus.DONE);
    ZonedDateTime limitTime = ZonedDateTime.now().plusSeconds(1);

    int count = taskDao.getStuckTasksCount(limitTime, 10);

    assertEquals(4, count);
  }

  @Test
  void gettingStuckTaskCountConsidersTheLimitTime() {
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);
    addRandomTask(TaskStatus.PROCESSING);
    addRandomTask(TaskStatus.PROCESSING);
    ZonedDateTime limitTime = ZonedDateTime.now(testClock).plus(1, ChronoUnit.MILLIS);
    testClock.tick(Duration.ofMillis(1));
    addRandomTask(TaskStatus.PROCESSING);

    int count = taskDao.getStuckTasksCount(limitTime, 10);

    assertEquals(2, count);
  }

  @Test
  void deletingAllTasksShouldDeleteAllTasks() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING);

    taskDao.deleteAllTasks();

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertNull(fullTaskRecord);
  }

  @Test
  void deletingTasksByTypeDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type);
    addRandomTask();

    taskDao.deleteTasks(type, null);

    List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null);
    assertEquals(0, deletedTasks.size());

    List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", null);
    assertEquals(1, remainingTasks.size());
  }

  @Test
  void deletingTasksByTypeAndSubtypeDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    String subType = "MY_SUBTYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type, subType);
    addTask(UUID.randomUUID(), TaskStatus.PROCESSING, "TEST", "SUBTYPE");

    taskDao.deleteTasks(type, subType);

    List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, subType);
    assertEquals(0, deletedTasks.size());
    List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", "SUBTYPE");
    assertEquals(1, remainingTasks.size());
  }

  @Test
  void deletingTasksByTypeAndStatusDeletesTheCorrectTasks() {
    String type = "MY_TYPE";
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.PROCESSING, type);
    addRandomTask();

    taskDao.deleteTasks(type, null, TaskStatus.PROCESSING);

    List<Task> deletedTasks = taskDao.findTasksByTypeSubTypeAndStatus(type, null, TaskStatus.PROCESSING);
    assertEquals(0, deletedTasks.size());
    List<Task> remainingTasks = taskDao.findTasksByTypeSubTypeAndStatus("TEST", null, TaskStatus.DONE);
    assertEquals(1, remainingTasks.size());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deletingOldTasksIsHappeningInBatches(boolean paranoid) {
    testTasksService.stopProcessing();
    tasksProperties.setParanoidTasksCleaning(paranoid);
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);

    addRandomTask();
    addRandomTask();

    testClock.tick(Duration.ofMinutes(11));
    DeleteFinishedOldTasksResult result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1);

    assertEquals(1, taskDao.getTasksCountInStatus(10, TaskStatus.DONE));
    assertEquals(1, result.getDeletedTasksCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());

    result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1);

    assertEquals(0, taskDao.getTasksCountInStatus(10, TaskStatus.DONE));
    assertEquals(1, result.getDeletedTasksCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deletingMultipleOldTasksIsReusingSqls(boolean paranoid) {
    testTasksService.stopProcessing();
    tasksProperties.setParanoidTasksCleaning(paranoid);
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);

    for (int i = 0; i < 117; i++) {
      taskDao.insertTask(new ITaskDao.InsertTaskRequest()
          .setType("Test").setData("Test").setPriority(5)
          .setMaxStuckTime(ZonedDateTime.now(TwContextClockHolder.getClock()))
          .setKey(UUID.randomUUID().toString())
          .setStatus(TaskStatus.DONE)
      );
    }

    testClock.tick(Duration.ofMinutes(11));
    DeleteFinishedOldTasksResult result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 60);

    assertEquals(117 - 60, taskDao.getTasksCountInStatus(1000, TaskStatus.DONE));
    assertEquals(117 - 60, getUniqueTaskKeysCount());
    assertEquals(60, result.getDeletedTasksCount());
    assertEquals(60, result.getDeletedUniqueKeysCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());

    result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 60);

    assertEquals(0, taskDao.getTasksCountInStatus(1000, TaskStatus.DONE));
    assertEquals(0, getUniqueTaskKeysCount());
    assertEquals(117 - 60, result.getDeletedTasksCount());
    assertEquals(117 - 60, result.getDeletedUniqueKeysCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());
  }

  @Test
  void deletingTaskByIdDeletedTheCorrectTask() {
    String type = "MY_TYPE";
    UUID taskId1 = UUID.randomUUID();
    addTask(taskId1, TaskStatus.PROCESSING, type);
    UUID taskId2 = UUID.randomUUID();
    addTask(taskId2, TaskStatus.PROCESSING, type);

    taskDao.deleteTask(taskId1, 0);

    FullTaskRecord task1 = taskDao.getTask(taskId1, FullTaskRecord.class);
    assertNull(task1);

    FullTaskRecord task2 = taskDao.getTask(taskId2, FullTaskRecord.class);
    assertEquals(taskId2, task2.getId());
  }

  @Test
  void gettingTasksInErrorStatusReturnsTasksInTheLimitOfMaxCount() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId, TaskStatus.ERROR, "TYPE", "1");
    addRandomTask(TaskStatus.ERROR, 2);
    addRandomTask(TaskStatus.PROCESSING, 3);
    addRandomTask(TaskStatus.ERROR, 4);

    List<ITaskDao.DaoTask1> tasks = taskDao.getTasksInErrorStatus(2);

    assertEquals(2, tasks.size());
    for (DaoTask1 task : tasks) {
      assertFalse(task.getSubType().contains("3"));
    }
  }

  @Test
  void gettingTasksInProcessingOrWaitingStatusReturnsTasksInTheLimitOfMaxCount() {
    addRandomTask(TaskStatus.PROCESSING, 1);
    addRandomTask(TaskStatus.ERROR, 2);
    addRandomTask(TaskStatus.WAITING, 3);
    addRandomTask(TaskStatus.PROCESSING, 4);
    addRandomTask(TaskStatus.WAITING, 5);

    List<ITaskDao.DaoTask3> tasks = taskDao.getTasksInProcessingOrWaitingStatus(3);

    assertEquals(3, tasks.size());
    assertEquals(3, tasks.stream().filter(t -> ImmutableSet.of("1", "3", "4", "5").contains(t.getSubType())).count());
    assertEquals(
        ImmutableSet.of(TaskStatus.PROCESSING.name(), TaskStatus.WAITING.name()),
        tasks.stream()
            .map(ITaskDao.DaoTask3::getStatus)
            .collect(Collectors.toSet())
    );
  }

  @Test
  void gettingStuckTasksReturnsTasksInTheLimitOfMaxCount() {
    addRandomTask(TaskStatus.PROCESSING);
    addRandomTask(TaskStatus.SUBMITTED);
    addRandomTask(TaskStatus.NEW);
    addRandomTask(TaskStatus.WAITING);
    addRandomTask(TaskStatus.DONE);
    addRandomTask(TaskStatus.WAITING);

    List<ITaskDao.DaoTask2> tasks = taskDao.getStuckTasks(4, Duration.ofMillis(-2));

    assertEquals(4, tasks.size());
  }

  @Test
  void clearingPayloadAndMarkingDoneUpdateTheTaskCorrectly() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId);

    boolean result = taskDao.clearPayloadAndMarkDone(taskId, 0);

    assertTrue(result);
    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertEquals("", task.getData());
  }

  @Test
  void gettingTaskVersionReturnTheCorrectNumber() {
    UUID taskId = UUID.randomUUID();
    addTask(taskId);
    addTask();

    Long version = taskDao.getTaskVersion(taskId);

    assertEquals(0, version);
  }

  @Test
  void gettingTasksByUuidListWorks() {
    final UUID taskId1 = UUID.randomUUID();
    final UUID taskId2 = UUID.randomUUID();
    final UUID taskId3 = UUID.randomUUID();
    final UUID taskId4 = UUID.randomUUID();
    final UUID taskId5 = UUID.randomUUID();
    final UUID taskId6 = UUID.randomUUID();
    final UUID taskId7 = UUID.randomUUID();

    addTask(taskId1);
    addTask();
    addTask(taskId2);
    addTask();
    addTask(taskId3);
    addTask(taskId4);
    addTask(taskId5);
    addTask(taskId6);
    addTask(taskId7);

    List<FullTaskRecord> tasks = taskDao.getTasks(Arrays.asList(taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7));

    assertEquals(7, tasks.size());
    assertEquals(
        ImmutableSet.of(taskId1, taskId2, taskId3, taskId4, taskId5, taskId6, taskId7),
        tasks.stream().map(FullTaskRecord::getId).collect(Collectors.toSet())
    );

    // task is correctly populated
    FullTaskRecord task1 = tasks.stream().filter(t -> t.getId().equals(taskId1)).findAny().get();
    assertEquals("DONE", task1.getStatus());
    assertEquals("TEST", task1.getType());
    assertEquals(5, task1.getPriority());
    assertEquals(0, task1.getVersion());
    assertEquals("DATA", task1.getData());
    assertEquals("SUBTYPE", task1.getSubType());
    assertEquals(0, task1.getProcessingTriesCount());
    assertNull(task1.getProcessingClientId());
    assertNotNull(task1.getStateTime());
    assertNotNull(task1.getNextEventTime());
  }

  private int getUniqueTaskKeysCount() {
    Integer cnt = jdbcTemplate.queryForObject("select count(*) from unique_tw_task_key", Integer.class);
    // Just keep the spotbugs happy.
    return cnt == null ? 0 : cnt;
  }

  private InsertTaskResponse addTask() {
    return addTask(UUID.randomUUID());
  }

  private InsertTaskResponse addTask(UUID id) {
    return addTask(id, TaskStatus.DONE);
  }

  private InsertTaskResponse addTask(UUID id, TaskStatus status) {
    return addTask(id, status, "TEST", "SUBTYPE", "DATA");
  }

  private InsertTaskResponse addTask(UUID id, TaskStatus status, String type) {
    return addTask(id, status, type, "SUBTYPE", "DATA");
  }

  private InsertTaskResponse addTask(UUID id, TaskStatus status, String type, String subType) {
    return addTask(id, status, type, subType, "DATA");
  }

  private InsertTaskResponse addTask(
      UUID id,
      TaskStatus taskStatus,
      String type,
      String subType,
      String data
  ) {
    return taskDao.insertTask(
        new ITaskDao.InsertTaskRequest()
            .setData(data)
            .setMaxStuckTime(ZonedDateTime.now(TwContextClockHolder.getClock()))
            .setTaskId(id)
            .setPriority(5)
            .setStatus(taskStatus)
            .setType(type)
            .setSubType(subType)
    );
  }

  private ITaskDao.InsertTaskResponse addRandomTask(TaskStatus taskStatus) {
    return addTask(UUID.randomUUID(), taskStatus);
  }

  private ITaskDao.InsertTaskResponse addRandomTask() {
    return addTask(UUID.randomUUID(), TaskStatus.DONE);
  }

  private ITaskDao.InsertTaskResponse addRandomTask(TaskStatus taskStatus, Integer id) {
    return addTask(UUID.randomUUID(), taskStatus, "TEST", id.toString());
  }

  private ITaskDao.InsertTaskResponse addRandomTask(TaskStatus taskStatus, Integer id, String type) {
    return addTask(UUID.randomUUID(), taskStatus, type, id == null ? "SUBTYPE" : id.toString());
  }
}
