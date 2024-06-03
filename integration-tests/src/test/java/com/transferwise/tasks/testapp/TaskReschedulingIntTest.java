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
import com.transferwise.tasks.ITasksService.RescheduleTaskResponse.Result;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.management.ITasksManagementService;
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
public class TaskReschedulingIntTest  extends BaseIntTest {

  @Autowired
  private ITasksService tasksService;
  @Autowired
  private ITestTasksService testTasksService;
  @Autowired
  private ITaskDataSerializer taskDataSerializer;
  @Autowired
  private ITasksManagementService tasksManagementService;
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
  void taskCanBeSuccessfullyRescheduled() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I want to be rescheduled"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());

    var task = tasksManagementService.getTasksById(
        new ITasksManagementService.GetTasksByIdRequest().setTaskIds(List.of(taskId))
    ).getTasks().stream().filter(t -> t.getTaskVersionId().getId().equals(taskId)).findFirst().orElseThrow();

    assertTrue(transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.rescheduleTask(
            new ITasksService.RescheduleTaskRequest()
                .setTaskId(taskId)
                .setVersion(task.getTaskVersionId().getVersion())
                .setRunAfterTime(ZonedDateTime.now().minusHours(1))
        ).getResult() == Result.OK
    ));

    await().until(() -> testTasksService.getTasks("test", null, WAITING).isEmpty());
    await().until(() -> resultRegisteringSyncTaskProcessor.getTaskResults().get(taskId) != null);
    assertEquals(0, getFailedNextEventTimeChangeCount());
    assertEquals(1, getTaskRescheduledCount());
  }

  @Test
  void taskWillNotBeRescheduleIfVersionHasAlreadyChanged() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    final long initialFailedNextEventTimeChangeCount = getFailedNextEventTimeChangeCount();
    final UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I want to be rescheduled too!"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());

    var task = tasksManagementService.getTasksById(
        new ITasksManagementService.GetTasksByIdRequest().setTaskIds(List.of(taskId))
    ).getTasks().stream().filter(t -> t.getTaskVersionId().getId().equals(taskId)).findFirst().orElseThrow();

    assertFalse(
        transactionsHelper.withTransaction().asNew().call(() ->
            tasksService.rescheduleTask(
                new ITasksService.RescheduleTaskRequest()
                    .setTaskId(taskId)
                    .setVersion(task.getTaskVersionId().getVersion() - 1)
                    .setRunAfterTime(ZonedDateTime.now().plusHours(2))
            ).getResult() == Result.OK
        )
    );
    assertEquals(initialFailedNextEventTimeChangeCount + 1, getFailedNextEventTimeChangeCount());
    assertEquals(0, getTaskRescheduledCount());
  }

  @ParameterizedTest
  @EnumSource(value = TaskStatus.class,
      names = {"WAITING", "UNKNOWN"},
      mode = EnumSource.Mode.EXCLUDE)
  void taskWillNotBeRescheduleIfNotWaiting(TaskStatus status) {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    final long initialFailedNextEventTimeChangeCount = getFailedNextEventTimeChangeCount();
    final UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setData(taskDataSerializer.serialize("I do not want to be rescheduled!"))
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(2)))
    );

    await().until(() -> !testTasksService.getWaitingTasks("test", null).isEmpty());
    List<Task> tasks = testTasksService.getWaitingTasks("test", null);
    Task task = tasks.stream().filter(t -> t.getId().equals(taskId)).findFirst().orElseThrow();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(task.getVersion()))
    );

    await().until(() -> testTasksService.getWaitingTasks("test", null).isEmpty());

    var updateTask = tasksManagementService.getTasksById(
        new ITasksManagementService.GetTasksByIdRequest().setTaskIds(List.of(taskId))
    ).getTasks().stream().filter(t -> t.getTaskVersionId().getId().equals(taskId)).findFirst().orElseThrow();

    taskDao.setStatus(taskId, status, updateTask.getTaskVersionId().getVersion());

    var finalTask = tasksManagementService.getTasksById(
        new ITasksManagementService.GetTasksByIdRequest().setTaskIds(List.of(taskId))
    ).getTasks().stream().filter(t -> t.getTaskVersionId().getId().equals(taskId)).findFirst().orElseThrow();

    assertFalse(
        transactionsHelper.withTransaction().asNew().call(() ->
            tasksService.rescheduleTask(
                new ITasksService.RescheduleTaskRequest()
                    .setTaskId(taskId)
                    .setVersion(finalTask.getTaskVersionId().getVersion())
                    .setRunAfterTime(ZonedDateTime.now().plusHours(2))
            ).getResult() == Result.OK
        )
    );
    assertEquals(initialFailedNextEventTimeChangeCount + 1, getFailedNextEventTimeChangeCount());
    assertEquals(0, getTaskRescheduledCount());
  }

  private long getFailedNextEventTimeChangeCount() {
    Counter counter = meterRegistry.find("twTasks.tasks.failedNextEventTimeChangeCount").tags(
        "taskType", "test"
    ).counter();

    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }

  private long getTaskRescheduledCount() {
    Counter counter = meterRegistry.find("twTasks.tasks.rescheduledCount").tags(
        "taskType", "test"
    ).counter();

    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }
}
