package com.transferwise.tasks.testapp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
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
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

public class TasksManagementPortIntTest extends BaseIntTest {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private TasksResumer tasksResumer;
  @Autowired
  private ITasksService tasksService;

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

  @SneakyThrows
  @WithMockUser(value = "a good user", authorities = {"RANDOM1", "NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY", "RANDOM2"})
  @ParameterizedTest(name = "find a task in {0} state")
  @EnumSource(value = TaskStatus.class, names = "UNKNOWN", mode = Mode.EXCLUDE)
  void findATaskInAGivenStatus(TaskStatus status) {
    testTasksService.stopProcessing();

    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(status)
            .withData("the payload")
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

    // test the endpoint with payload data
    mockMvc.perform(MockMvcRequestBuilders.get("/v1/twTasks/task/" + taskId + "/data"))
        .andExpect(status().isOk())
        .andExpect(content().string("{\"data\":\"the payload\",\"resultCode\":\"SUCCESS\"}"))
        .andExpect(content().contentType("application/json"));
  }

  @SneakyThrows
  @Test
  @WithMockUser(value = "a funky user", authorities = "READ_MINDS")
  void wrongRoleCannotViewTaskData() {
    mockMvc.perform(MockMvcRequestBuilders.get("/v1/twTasks/task/99e30d10-a085-4708-9591-d5159ff1056c/data"))
        .andExpect(status().isForbidden())
        .andExpect(content().string(""));
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

    testTasksService.stopProcessing();

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

    testTasksService.stopProcessing();

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

}
