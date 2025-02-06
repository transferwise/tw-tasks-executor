package com.transferwise.tasks.testapp;

import static com.transferwise.tasks.domain.TaskStatus.WAITING;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.GetTaskRequest;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.test.ITestTasksService;
import io.micrometer.core.instrument.Counter;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TaskDeletionIntTest extends BaseIntTest {

  @Autowired
  private ITasksService tasksService;
  @Autowired
  private ITestTasksService testTasksService;
  @Autowired
  private ITaskDataSerializer taskDataSerializer;
  @Autowired
  private ITaskDao taskDao;

  @BeforeEach
  void setup() {
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });
  }

  @Test
  void taskCanBeSuccessfullyDeleted() {
    UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I want to be deleted"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());

    var task = tasksService.getTask(new GetTaskRequest().setTaskId(taskId));

    assertTrue(transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.deleteTask(
            new ITasksService.DeleteTaskRequest()
                .setTaskId(taskId)
                .setVersion(task.getVersion())
        )
    ));

    await().until(() -> testTasksService.getTasks("test", null, WAITING).isEmpty());
    assertEquals(0, getFailedDeletionCount());
    assertEquals(1, getTaskDeletedCount());
  }

  @Test
  void taskWillNotBeDeletedIfVersionHasAlreadyChanged() {
    final long initialFailedNextEventTimeChangeCount = getFailedDeletionCount();
    final UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I want to be deleted too!"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());

    var task = tasksService.getTask(new GetTaskRequest().setTaskId(taskId));

    assertFalse(
        transactionsHelper.withTransaction().asNew().call(() ->
            tasksService.deleteTask(
                new ITasksService.DeleteTaskRequest()
                    .setTaskId(taskId)
                    .setVersion(task.getVersion() - 1)
            )
        )
    );

    assertEquals(initialFailedNextEventTimeChangeCount + 1, getFailedDeletionCount());
    assertEquals(0, getTaskDeletedCount());
  }

  @ParameterizedTest
  @EnumSource(value = TaskStatus.class,
      names = {"PROCESSING", "UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void taskWillBeDeletedForAnyStatusExceptProcessing(TaskStatus status) {
    final long initialFailedNextEventTimeChangeCount = getFailedDeletionCount();
    final UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I want to be deleted!"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusDays(12)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());
    List<Task> tasks = testTasksService.getWaitingTasks("test", null);
    Task task = tasks.stream().filter(t -> t.getId().equals(taskId)).findFirst().orElseThrow();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(task.getVersion()))
    );

    var updateTask = tasksService.getTask(new GetTaskRequest().setTaskId(taskId));

    taskDao.setStatus(taskId, status, updateTask.getVersion());

    var finalTask = tasksService.getTask(new GetTaskRequest().setTaskId(taskId));

    assertTrue(
        transactionsHelper.withTransaction().asNew().call(() ->
            tasksService.deleteTask(
                new ITasksService.DeleteTaskRequest()
                    .setTaskId(taskId)
                    .setVersion(finalTask.getVersion())
            )
        )
    );
    assertEquals(initialFailedNextEventTimeChangeCount, getFailedDeletionCount());
    assertEquals(1, getTaskDeletedCount());
  }

  private long getFailedDeletionCount() {
    Counter counter = meterRegistry.find("twTasks.tasks.failedDeletionCount").tags(
        "taskType", "test"
    ).counter();

    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }

  private long getTaskDeletedCount() {
    Counter counter = meterRegistry.find("twTasks.tasks.deletedCount").tags(
        "taskType", "test"
    ).counter();

    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }
}
