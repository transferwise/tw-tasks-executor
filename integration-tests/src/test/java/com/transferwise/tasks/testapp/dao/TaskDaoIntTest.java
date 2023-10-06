package com.transferwise.tasks.testapp.dao;

import static com.transferwise.tasks.TaskTestBuilder.randomDoneTask;
import static com.transferwise.tasks.TaskTestBuilder.randomErrorTask;
import static com.transferwise.tasks.TaskTestBuilder.randomNewTask;
import static com.transferwise.tasks.TaskTestBuilder.randomProcessingTask;
import static com.transferwise.tasks.TaskTestBuilder.randomSubmittedTask;
import static com.transferwise.tasks.TaskTestBuilder.randomWaitingTask;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.spyql.SpyqlDataSource;
import com.transferwise.common.spyql.event.StatementExecuteEvent;
import com.transferwise.common.spyql.listener.SpyqlConnectionListener;
import com.transferwise.common.spyql.listener.SpyqlDataSourceListener;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.TaskTestBuilder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDao.DeleteFinishedOldTasksResult;
import com.transferwise.tasks.dao.ITaskDao.GetStuckTasksResponse;
import com.transferwise.tasks.dao.ITaskDao.InsertTaskRequest;
import com.transferwise.tasks.dao.ITaskDao.InsertTaskResponse;
import com.transferwise.tasks.dao.ITaskDao.StuckTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.test.dao.ITestTaskDao;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.data.TemporalUnitWithinOffset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

abstract class TaskDaoIntTest extends BaseIntTest {

  @Autowired
  protected ITaskDao taskDao;
  @Autowired
  private JdbcTemplate jdbcTemplate;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private ITestTaskDao testTaskDao;
  @Autowired
  private ITaskDataSerializer taskDataSerializer;
  @Autowired
  protected DataSource dataSource;

  @BeforeEach
  @SneakyThrows
  void taskDaoIntTestSetup() {
    testTasksService.stopProcessing();
  }

  @AfterEach
  void taskDaoIntTestCleanup() {
    tasksProperties.setParanoidTasksCleaning(false);
  }

  @Test
  void insertingATaskInsertsOnlyOneTaskForAGivenId() {
    UUID taskId = UuidUtils.generatePrefixCombUuid();

    InsertTaskResponse result1 = randomSubmittedTask().withId(taskId).save();
    InsertTaskResponse result2 = randomDoneTask().withId(taskId).save();

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

    InsertTaskResponse response = TaskTestBuilder.newTask()
        .withStatus(TaskStatus.NEW)
        .withKey(key)
        .withPriority(5)
        .withMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
        .withType("Test")
        .save();

    assertNotNull(response.getTaskId());

    response = taskDao.insertTask(new ITaskDao.InsertTaskRequest()
        .setStatus(TaskStatus.NEW)
        .setKey(key)
        .setMaxStuckTime(ZonedDateTime.now().plusMinutes(30))
        .setType("Test")
        .setPriority(5));

    assertFalse(response.isInserted());
    assertNull(response.getTaskId());
  }

  @Test
  void gettingATaskReturnsTheCorrectTaskForBaseTask1() {
    UUID taskId = randomDoneTask().save().getTaskId();
    randomDoneTask().save();

    BaseTask1 task = taskDao.getTask(taskId, BaseTask1.class);

    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertEquals("TEST", task.getType());
    assertEquals(5, task.getPriority());
    assertEquals(0, task.getVersion());
  }

  @Test
  void gettingATaskReturnsTehCorrectTaskForFullTaskRecord() {
    UUID taskId = randomDoneTask().save().getTaskId();
    randomDoneTask().save();

    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);

    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertEquals("TEST", task.getType());
    assertEquals(5, task.getPriority());
    assertEquals(0, task.getVersion());
    assertThat(task.getData()).isEqualTo(taskDataSerializer.serialize("DATA"));
    assertEquals("SUBTYPE", task.getSubType());
    assertEquals(0, task.getProcessingTriesCount());
    assertNull(task.getProcessingClientId());
    assertNotNull(task.getStateTime());
    assertNotNull(task.getNextEventTime());
  }

  @Test
  void settingToBeRetriedSetsTheCorrectRetryTime() {
    ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2);
    UUID taskId = randomDoneTask().save().getTaskId();

    boolean result = taskDao.setToBeRetried(taskId, retryTime, 0, false);

    assertTrue(result);

    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("WAITING", task.getStatus());
    assertEquals(1, task.getVersion());
    assertEquals(0, task.getProcessingTriesCount());
    assertNotNull(task.getStateTime());
    assertThat(retryTime).isCloseTo(task.getNextEventTime(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
  }

  @Test
  void grabbingForProcessingIncrementsTheProcessingTriesCount() {
    String nodeId = "testNode";
    Instant processingDeadline = Instant.now().plus(Duration.ofHours(2));
    UUID taskId = randomSubmittedTask().save().getTaskId();
    Task task = taskDao.getTask(taskId, Task.class);

    Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    assertEquals(taskId, returnedTask.getId());
    assertEquals("PROCESSING", returnedTask.getStatus());
    assertEquals(1, returnedTask.getVersion());
    assertEquals(1, returnedTask.getProcessingTriesCount());

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertThat(processingDeadline).isCloseTo(fullTaskRecord.getNextEventTime().toInstant(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
    assertEquals(nodeId, fullTaskRecord.getProcessingClientId());
  }

  @Test
  void settingToBeRetriedResetTriesCountIfSpecified() {
    String nodeId = "testNode";
    ZonedDateTime retryTime = ZonedDateTime.now().plusHours(2);
    UUID taskId = randomSubmittedTask().save().getTaskId();
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
    assertThat(finalTask.getNextEventTime()).isCloseTo(retryTime.toString(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
  }

  @Test
  void grabbingForProcessingReturnsNullIfNoTaskMatchesTheOnePassed() {
    String nodeId = "testNode";
    Instant processingDeadline = Instant.now().plus(Duration.ofHours(2));
    UUID taskId = randomSubmittedTask().save().getTaskId();
    Task task = new Task().setId(UuidUtils.generatePrefixCombUuid());

    Task returnedTask = taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    assertNull(returnedTask);

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(0, fullTaskRecord.getVersion());
    assertEquals(0, fullTaskRecord.getProcessingTriesCount());
  }

  @Test
  void settingStatusUpdatesTheStatusCorrectly() {
    UUID taskId = randomSubmittedTask().save().getTaskId();

    boolean result = taskDao.setStatus(taskId, TaskStatus.WAITING, 0);

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
    final UUID taskId = randomSubmittedTask().save().getTaskId();
    final ZonedDateTime oldNextEventTime = taskDao.getTask(taskId, FullTaskRecord.class).getNextEventTime();
    randomSubmittedTask().save();
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
    UUID taskId = randomSubmittedTask().save().getTaskId();
    randomNewTask().save();

    boolean result = taskDao.markAsSubmitted(taskId, 0, maxStuckTime);

    assertTrue(result);
    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertEquals(1, fullTaskRecord.getVersion());
    assertThat(maxStuckTime).isCloseTo(fullTaskRecord.getNextEventTime(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
  }

  @Test
  void preparingStuckOnProcessingTasksForResumingPutsTheTaskInSubmittedState() {
    Instant processingDeadline = ZonedDateTime.now().plusHours(2).toInstant();
    UUID taskId = randomSubmittedTask().save().getTaskId();
    String nodeId = "testNode";
    Task task = taskDao.getTask(taskId, Task.class);
    taskDao.grabForProcessing(task.toBaseTask(), nodeId, processingDeadline);

    List<StuckTask> tasks = taskDao.prepareStuckOnProcessingTasksForResuming(
        nodeId, ZonedDateTime.ofInstant(processingDeadline, ZoneId.systemDefault())
    );

    assertEquals(1, tasks.size());
    assertEquals(2, tasks.get(0).getVersionId().getVersion());

    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertThat(processingDeadline).isCloseTo(fullTaskRecord.getNextEventTime().toInstant(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
  }

  @Test
  void markingAsSubmittedUpdatesTheStateCorrectly() {
    ZonedDateTime maxStuckTime = ZonedDateTime.now().plusHours(2);
    UUID taskId = randomProcessingTask().save().getTaskId();

    boolean result = taskDao.markAsSubmitted(taskId, 0, maxStuckTime);

    assertTrue(result);
    FullTaskRecord fullTaskRecord = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals("SUBMITTED", fullTaskRecord.getStatus());
    assertThat(maxStuckTime.toInstant()).isCloseTo(fullTaskRecord.getNextEventTime().toInstant(), new TemporalUnitWithinOffset(1, ChronoUnit.MICROS));
  }

  @Test
  void gettingTaskCountInStatusReturnsTheCorrectValue() {
    randomProcessingTask().save();
    randomProcessingTask().save();
    randomDoneTask().save();

    int count = taskDao.getTasksCountInStatus(10, TaskStatus.PROCESSING);

    assertEquals(2, count);
  }

  @Test
  void gettingErrorTasksCountByType() {
    randomDoneTask().save();
    randomErrorTask().save();
    randomErrorTask().save();
    randomErrorTask().withType("XXX").save();

    Map<String, Integer> tasksInErrorStatus = taskDao.getErronousTasksCountByType(10);

    assertEquals(2, tasksInErrorStatus.size());
    assertThat(tasksInErrorStatus.get("TEST")).isEqualTo(2);
    assertThat(tasksInErrorStatus.get("XXX")).isEqualTo(1);
    assertThat(tasksInErrorStatus.get("OOO")).isNull();
  }

  @Test
  void gettingStuckTaskCountConsidersTheCorrectStatus() {
    randomProcessingTask().save();
    randomSubmittedTask().save();
    randomNewTask().save();
    randomWaitingTask().save();
    randomDoneTask().save();

    ZonedDateTime limitTime = ZonedDateTime.now().plusSeconds(1);

    int count = taskDao.getStuckTasksCount(limitTime, 10);

    assertEquals(4, count);
  }

  @Test
  void gettingStuckTasksCountByType() {
    randomProcessingTask().save();
    randomSubmittedTask().withType("XXX").save();
    randomNewTask().save();
    randomWaitingTask().save();
    randomDoneTask().save();

    ZonedDateTime limitTime = ZonedDateTime.now().plusSeconds(1);

    Map<Pair<TaskStatus, String>, Integer> stuckTask = taskDao.getStuckTasksCountByStatusAndType(limitTime, 10);
    assertThat(stuckTask).size().isEqualTo(4);
    assertThat(stuckTask.get(Pair.of(TaskStatus.NEW, "TEST"))).isEqualTo(1);
    assertThat(stuckTask.get(Pair.of(TaskStatus.WAITING, "TEST"))).isEqualTo(1);
    assertThat(stuckTask.get(Pair.of(TaskStatus.PROCESSING, "TEST"))).isEqualTo(1);
    assertThat(stuckTask.get(Pair.of(TaskStatus.SUBMITTED, "XXX"))).isEqualTo(1);
  }

  @Test
  void gettingStuckTaskCountConsidersTheLimitTime() {
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);
    randomProcessingTask().save();
    randomProcessingTask().save();
    ZonedDateTime limitTime = ZonedDateTime.now(testClock).plus(1, ChronoUnit.MILLIS);
    testClock.tick(Duration.ofMillis(1));
    randomProcessingTask().save();

    int count = taskDao.getStuckTasksCount(limitTime, 10);

    assertEquals(2, count);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void deletingOldTasksIsHappeningInBatches(boolean paranoid) {
    testTasksService.stopProcessing();
    tasksProperties.setParanoidTasksCleaning(paranoid);
    final TestClock testClock = new TestClock();
    TwContextClockHolder.setClock(testClock);

    randomDoneTask().save();
    randomDoneTask().save();

    testClock.tick(Duration.ofMinutes(11));
    DeleteFinishedOldTasksResult result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1);

    assertEquals(1, taskDao.getTasksCountInStatus(10, TaskStatus.DONE));
    assertEquals(1, result.getDeletedTasksCount());
    assertEquals(0, result.getDeletedUniqueKeysCount());
    assertEquals(1, result.getDeletedTaskDatasCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());

    result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 1);

    assertEquals(0, taskDao.getTasksCountInStatus(10, TaskStatus.DONE));
    assertEquals(1, result.getDeletedTasksCount());
    assertEquals(1, result.getDeletedTaskDatasCount());
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
      taskDao.insertTask(new InsertTaskRequest()
          .setType("Test").setData(taskDataSerializer.serialize("Test")).setPriority(5)
          .setMaxStuckTime(ZonedDateTime.now(TwContextClockHolder.getClock()))
          .setKey(UuidUtils.generatePrefixCombUuid().toString())
          .setStatus(TaskStatus.DONE)
      );
    }

    testClock.tick(Duration.ofMinutes(11));
    final DeleteFinishedOldTasksResult result = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 60);

    assertEquals(60, result.getDeletedTasksCount());
    assertEquals(60, result.getDeletedUniqueKeysCount());
    assertEquals(60, result.getDeletedTaskDatasCount());

    assertEquals(117 - 60, taskDao.getTasksCountInStatus(1000, TaskStatus.DONE));
    assertEquals(117 - 60, getUniqueTaskKeysCount());
    assertEquals(117 - 60, getTaskDatasCount());
    assertNotNull(result.getFirstDeletedTaskNextEventTime());

    final DeleteFinishedOldTasksResult result1 = taskDao.deleteOldTasks(TaskStatus.DONE, Duration.ofMinutes(10), 60);

    assertEquals(0, taskDao.getTasksCountInStatus(1000, TaskStatus.DONE));
    assertEquals(0, getUniqueTaskKeysCount());
    assertEquals(0, getTaskDatasCount());
    assertEquals(117 - 60, result1.getDeletedTasksCount());
    assertEquals(117 - 60, result1.getDeletedUniqueKeysCount());
    assertEquals(117 - 60, result1.getDeletedTaskDatasCount());
    assertNotNull(result1.getFirstDeletedTaskNextEventTime());
  }

  @Test
  void deletingTaskByIdDeletedTheCorrectTask() {
    final String type = "MY_TYPE";
    final UUID taskId1 = randomProcessingTask().withType(type).save().getTaskId();
    final UUID taskId2 = randomProcessingTask().withType(type).save().getTaskId();

    taskDao.deleteTask(taskId1, 0);

    FullTaskRecord task1 = taskDao.getTask(taskId1, FullTaskRecord.class);
    assertNull(task1);
    assertThat(testTaskDao.getSerializedData(taskId1)).isNull();

    FullTaskRecord task2 = taskDao.getTask(taskId2, FullTaskRecord.class);
    assertEquals(taskId2, task2.getId());
    assertThat(testTaskDao.getSerializedData(taskId2)).isNotNull();
  }

  @Test
  void clearingPayloadAndMarkingDoneUpdateTheTaskCorrectly() {
    UUID taskId = randomDoneTask().save().getTaskId();
    FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertThat(task.getData()).isNotNull();

    boolean result = taskDao.clearPayloadAndMarkDone(taskId, 0);

    assertTrue(result);
    task = taskDao.getTask(taskId, FullTaskRecord.class);
    assertEquals(taskId, task.getId());
    assertEquals("DONE", task.getStatus());
    assertThat(task.getData()).isNull();
  }

  @Test
  void gettingTaskVersionReturnTheCorrectNumber() {
    UUID taskId = randomDoneTask().save().getTaskId();
    randomDoneTask().save();

    Long version = taskDao.getTaskVersion(taskId);

    assertEquals(0, version);
  }

  @Test
  @SneakyThrows
  public void testUuids() {
    TestClock clock = TestClock.createAndRegister();
    String taskType = "UUID_TEST";
    try {
      for (int i = 0; i < 117; i++) {
        taskDao.insertTask(new InsertTaskRequest()
            .setType(taskType).setData(taskDataSerializer.serialize(String.valueOf(i))).setPriority(5)
            .setMaxStuckTime(ZonedDateTime.now(TwContextClockHolder.getClock()))
            .setTaskId(UuidUtils.generatePrefixCombUuid())
            .setStatus(TaskStatus.DONE)
        );
        clock.tick(Duration.ofMillis(2));
      }
      List<Task> tasks = testTaskDao.findTasksByTypeSubTypeAndStatus(taskType, null, TaskStatus.DONE);
      tasks.sort(Comparator.comparing(Task::getId));

      List<Integer> resultInts = tasks.stream().map(t -> Integer.parseInt(new String(t.getData(), StandardCharsets.UTF_8)))
          .collect(Collectors.toList());
      assertThat(resultInts).isSorted();
    } finally {
      TestClock.reset();
    }
  }

  @Test
  @SneakyThrows
  void approximateTaskCountCanBeRetrieved() {
    var correctSqlEncountered = new AtomicBoolean();
    var spyqlDataSource = dataSource.unwrap(SpyqlDataSource.class);
    final SpyqlDataSourceListener spyqlDataSourceListener = event -> new SpyqlConnectionListener() {
      @Override
      public void onStatementExecute(StatementExecuteEvent event) {
        // Correct schema is used.
        if (event.getSql().equals("select table_rows from information_schema.tables where table_schema='tw-tasks-test' and table_name = 'tw_task'")) {
          correctSqlEncountered.set(true);
        }
      }
    };

    spyqlDataSource.addListener(spyqlDataSourceListener);

    try {
      assertThat(taskDao.getApproximateTasksCount()).isGreaterThan(-1);
      assertThat(correctSqlEncountered).isTrue();
    } finally {
      spyqlDataSource.getDataSourceListeners().remove(spyqlDataSourceListener);
    }
  }

  @Test
  void approximateUniqueKeysCountCanBeRetrieved() {
    assertThat(taskDao.getApproximateUniqueKeysCount()).isGreaterThan(-1);
  }

  private int getUniqueTaskKeysCount() {
    Integer cnt = jdbcTemplate.queryForObject("select count(*) from unique_tw_task_key", Integer.class);
    // Just keep the spotbugs happy.
    return cnt == null ? 0 : cnt;
  }

  private int getTaskDatasCount() {
    Integer cnt = jdbcTemplate.queryForObject("select count(*) from tw_task_data", Integer.class);
    // Just keep the spotbugs happy.
    return cnt == null ? 0 : cnt;
  }
}
