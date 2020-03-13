package com.transferwise.tasks.testapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.TaskTestBuilder;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.management.ITasksManagementPort;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInErrorResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInErrorResponse.TaskInError;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksStuckResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksStuckResponse.TaskStuck;
import com.transferwise.tasks.stucktasks.TasksResumer;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

public class TasksManagementIntTest extends BaseIntTest {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private TasksResumer tasksResumer;

  private boolean resumerPaused;

  @AfterEach
  void cleanup() {
    if (resumerPaused) {
      tasksResumer.resume();
      resumerPaused = false;
    }
  }

  @Test
  void erroneousTasksWillBeCorrectlyFound() {
    transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .build()
    );

    final UUID task1Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withType("T1")
            .withSubType("S1")
            .withMaxStuckTime(ZonedDateTime.now().plusDays(2))
            .build()
    );

    ResponseEntity<GetTasksInErrorResponse> response = testRestTemplate.postForEntity(
        "/v1/twTasks/getTasksInError",
        new ITasksManagementPort.GetTasksInErrorRequest().setMaxCount(1),
        GetTasksInErrorResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTasksInErrorResponse tasksInErrorResponse = response.getBody();
    assertNotNull(tasksInErrorResponse);
    List<TaskInError> tasksInError = tasksInErrorResponse.getTasksInError();
    assertEquals(1, tasksInError.size());
    assertEquals(task1Id, tasksInError.get(0).getTaskVersionId().getId());
    assertEquals("T1", tasksInError.get(0).getType());
    assertEquals("S1", tasksInError.get(0).getSubType());
  }

  @Test
  void stuckTaskWillBeCorrectlyFound() {
    transactionsHelper.withTransaction().asNew().call(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2)).build();
      // It should not be found as we have 10s delta by default.
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now()).build();
      return null;
    });
    final UUID task1Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.SUBMITTED)
            .withMaxStuckTime(ZonedDateTime.now().minusDays(1))
            .build()
    );

    ResponseEntity<ITasksManagementPort.GetTasksStuckResponse> response = testRestTemplate.postForEntity(
        "/v1/twTasks/getTasksStuck",
        new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(1),
        ITasksManagementPort.GetTasksStuckResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTasksStuckResponse stuckTasksResponse = response.getBody();
    assertNotNull(stuckTasksResponse);
    List<TaskStuck> tasksStuck = stuckTasksResponse.getTasksStuck();
    assertEquals(tasksStuck.size(), 1);
    assertEquals(tasksStuck.get(0).getTaskVersionId().getId(), task1Id);

    response = testRestTemplate.postForEntity(
        "/v1/twTasks/getTasksStuck",
        new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(10),
        ITasksManagementPort.GetTasksStuckResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(2, response.getBody().getTasksStuck().size());
  }

  @ParameterizedTest(name = "find a task in {0} state")
  @EnumSource(value = TaskStatus.class, names = "UNKNOWN", mode = Mode.EXCLUDE)
  void findATaskInAGivenStatus(TaskStatus status) {
    pauseResumer();

    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(status)
            .withMaxStuckTime(ZonedDateTime.now().minusDays(2))
            .build()
    );

    ResponseEntity<ITasksManagementPort.TaskWithoutData> response = testRestTemplate.getForEntity(
        "/v1/twTasks/task/" + taskId + "/noData",
        ITasksManagementPort.TaskWithoutData.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(taskId.toString(), response.getBody().getId());
    assertEquals("test", response.getBody().getType());
    assertEquals(status.name(), response.getBody().getStatus());
  }

  @Test
  void markingATaskAsFailedWorks() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .build()
    );

    ResponseEntity<ITasksManagementPort.MarkTasksAsFailedResponse> response = testRestTemplate.postForEntity(
        "/v1/twTasks/markTasksAsFailed",
        new ITasksManagementPort.MarkTasksAsFailedRequest()
            .addTaskVersionId(new TaskVersionId().setId(task0Id).setVersion(0)),
        ITasksManagementPort.MarkTasksAsFailedResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertTrue(response.getBody().getResults().get(task0Id).isSuccess());
    FullTaskRecord task = taskDao.getTask(task0Id, FullTaskRecord.class);
    assertEquals(TaskStatus.FAILED.name(), task.getStatus());
  }

  @Test
  void immediatelyResumingATaskWorks() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .build()
    );

    pauseResumer();

    ResponseEntity<ITasksManagementPort.ResumeTasksImmediatelyResponse> response = testRestTemplate.postForEntity(
        "/v1/twTasks/resumeTasksImmediately",
        new ITasksManagementPort.ResumeTasksImmediatelyRequest()
            .addTaskVersionId(new TaskVersionId().setId(task0Id).setVersion(0)),
        ITasksManagementPort.ResumeTasksImmediatelyResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertTrue(response.getBody().getResults().get(task0Id).isSuccess());
    FullTaskRecord task = taskDao.getTask(task0Id, FullTaskRecord.class);
    assertEquals(TaskStatus.WAITING.name(), task.getStatus());
    // decrementing 1 because of database rounding error.
    assertFalse(task.getNextEventTime().isAfter(ZonedDateTime.now(ClockHolder.getClock()).plusSeconds(1)));
  }

  @Test
  void immediatelyResumingAllTasksWorks() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .build()
    );

    pauseResumer();

    ResponseEntity<ITasksManagementPort.ResumeTasksImmediatelyResponse> response = testRestTemplate.postForEntity(
        "/v1/twTasks/resumeAllTasksImmediately",
        new ITasksManagementPort.ResumeAllTasksImmediatelyRequest().setTaskType("test"),
        ITasksManagementPort.ResumeTasksImmediatelyResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertTrue(response.getBody().getResults().get(task0Id).isSuccess());
    FullTaskRecord task = taskDao.getTask(task0Id, FullTaskRecord.class);
    // decrementing 1 because of database rounding error
    assertFalse(task.getNextEventTime().isAfter(ZonedDateTime.now(ClockHolder.getClock()).plusSeconds(1)));
  }

  private void pauseResumer() {
    tasksResumer.pause();
    resumerPaused = true;
  }
}
