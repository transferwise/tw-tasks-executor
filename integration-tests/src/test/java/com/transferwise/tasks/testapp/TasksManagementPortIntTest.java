package com.transferwise.tasks.testapp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.TaskTestBuilder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.management.ITasksManagementPort;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse.ResultCode;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskTypesResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskWithoutDataResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInErrorResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInErrorResponse.TaskInError;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInProcessingOrWaitingResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksInProcessingOrWaitingResponse.TaskInProcessingOrWaiting;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksStuckResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTasksStuckResponse.TaskStuck;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class TasksManagementPortIntTest extends BaseIntTest {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private ITaskDataSerializer taskDataSerializer;
  @Autowired
  private TasksProperties tasksProperties;

  @Test
  void erroneousTasksWillBeCorrectlyFound() {
    transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
    );

    final UUID task1Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withType("T1")
            .withSubType("S1")
            .withMaxStuckTime(ZonedDateTime.now().plusDays(2))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTasksInErrorResponse> response = goodEngineerTemplate().postForEntity(
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
  void crackerCantGetErroneousTasks() {
    ResponseEntity<GetTasksInErrorResponse> response = badEngineerTemplate().postForEntity("/v1/twTasks/getTasksInError",
        new ITasksManagementPort.GetTasksInErrorRequest().setMaxCount(1), GetTasksInErrorResponse.class
    );

    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  @SneakyThrows
  void stuckTaskWillBeCorrectlyFound() {
    testTasksService.stopProcessing();

    transactionsHelper.withTransaction().asNew().call(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2)).save();
      // It should not be found as we have 10s delta by default.
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now()).save();
      return null;
    });
    final UUID task1Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.SUBMITTED)
            .withMaxStuckTime(ZonedDateTime.now().minusDays(1))
            .save()
            .getTaskId()
    );

    ResponseEntity<ITasksManagementPort.GetTasksStuckResponse> response = goodEngineerTemplate().postForEntity(
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

    response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/getTasksStuck",
        new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(10),
        ITasksManagementPort.GetTasksStuckResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(2, response.getBody().getTasksStuck().size());
  }

  @SneakyThrows
  @ParameterizedTest(name = "find a task in {0} state")
  @EnumSource(value = TaskStatus.class, names = "UNKNOWN", mode = Mode.EXCLUDE)
  void findATaskInAGivenStatus(TaskStatus status) {
    testTasksService.stopProcessing();

    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(status)
            .withData(taskDataSerializer.serialize("the payload"))
            .withMaxStuckTime(ZonedDateTime.now().minusDays(2))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskWithoutDataResponse> response = goodEngineerTemplate().getForEntity(
        "/v1/twTasks/task/" + taskId + "/noData",
        GetTaskWithoutDataResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());
    assertEquals(taskId, response.getBody().getId());
    assertEquals("test", response.getBody().getType());
    assertEquals(status.name(), response.getBody().getStatus());

    ResponseEntity<GetTaskDataResponse> dataResponse = piiOfficerTemplate()
        .getForEntity("/v1/twTasks/task/" + taskId + "/data", GetTaskDataResponse.class);
    assertThat(dataResponse.getBody()).isNotNull();
    assertThat(dataResponse.getBody().getData()).isEqualTo("the payload");
    assertThat(dataResponse.getBody().getResultCode()).isEqualTo(ResultCode.SUCCESS);
  }

  @SneakyThrows
  @ParameterizedTest(name = "find a task with {0} payload")
  @ValueSource(strings = {"Hello World!", "", "<null>"})
  void findATaskWithOrWithoutData(String payload) {
    if ("<null>".equals(payload)) {
      payload = null;
    }
    testTasksService.stopProcessing();

    String finalPayload = payload;
    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.DONE)
            .withData(taskDataSerializer.serialize(finalPayload))
            .withMaxStuckTime(ZonedDateTime.now().minusDays(2))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskWithoutDataResponse> response = goodEngineerTemplate().getForEntity(
        "/v1/twTasks/task/" + taskId + "/noData",
        GetTaskWithoutDataResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertNotNull(response.getBody());

    ResponseEntity<GetTaskDataResponse> dataResponse = piiOfficerTemplate()
        .getForEntity("/v1/twTasks/task/" + taskId + "/data", GetTaskDataResponse.class);
    assertThat(dataResponse.getBody()).isNotNull();
    assertThat(dataResponse.getBody().getData()).isEqualTo(payload);
    assertThat(dataResponse.getBody().getResultCode()).isEqualTo(ResultCode.SUCCESS);
  }

  @SneakyThrows
  @Test
  void getTaskDataInBase64() {
    testTasksService.stopProcessing();

    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.DONE)
            .withData(taskDataSerializer.serialize("Hello World!"))
            .withMaxStuckTime(ZonedDateTime.now().minusDays(2))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskDataResponse> dataResponse = piiOfficerTemplate()
        .getForEntity("/v1/twTasks/task/" + taskId + "/data?format=BaSe64", GetTaskDataResponse.class);
    assertThat(dataResponse.getBody()).isNotNull();
    assertThat(dataResponse.getBody().getData()).isEqualTo("SGVsbG8gV29ybGQh");
    assertThat(dataResponse.getBody().getResultCode()).isEqualTo(ResultCode.SUCCESS);
  }

  @SneakyThrows
  @Test
  void wrongRoleCannotViewTaskData() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskDataResponse> dataResponse = goodEngineerTemplate()
        .getForEntity("/v1/twTasks/task/" + task0Id + "/data", GetTaskDataResponse.class);

    assertThat(dataResponse.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);

    dataResponse = badEngineerTemplate()
        .getForEntity("/v1/twTasks/task/" + task0Id + "/data", GetTaskDataResponse.class);

    assertThat(dataResponse.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @SneakyThrows
  @Test
  void wrongRoleCannotViewTaskDataForTypeSpecificConfiguration() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .withType("customType")
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskDataResponse> dataResponse = badEngineerTemplate()
        .getForEntity("/v1/twTasks/task/" + task0Id + "/data", GetTaskDataResponse.class);

    assertThat(dataResponse.getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
  }

  @SneakyThrows
  @Test
  void rightRoleCanViewTaskDataForTypeSpecificConfiguration() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .withType("customType")
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
            .getTaskId()
    );

    ResponseEntity<GetTaskDataResponse> dataResponse = goodEngineerTemplate()
        .getForEntity("/v1/twTasks/task/" + task0Id + "/data", GetTaskDataResponse.class);

    assertThat(dataResponse.getStatusCode()).isEqualTo(HttpStatus.OK);
  }

  @Test
  void markingATaskAsFailedWorks() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
            .getTaskId()
    );

    ResponseEntity<ITasksManagementPort.MarkTasksAsFailedResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/markTasksAsFailed",
        new ITasksManagementPort.MarkTasksAsFailedRequest()
            .addTaskVersionId(new TaskVersionId().setId(task0Id).setVersion(0)),
        ITasksManagementPort.MarkTasksAsFailedResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertThat(response.getBody()).isNotNull();
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
            .save()
            .getTaskId()
    );

    testTasksService.stopProcessing();

    ResponseEntity<ITasksManagementPort.ResumeTasksImmediatelyResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/resumeTasksImmediately",
        new ITasksManagementPort.ResumeTasksImmediatelyRequest()
            .addTaskVersionId(new TaskVersionId().setId(task0Id).setVersion(0)),
        ITasksManagementPort.ResumeTasksImmediatelyResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertThat(response.getBody()).isNotNull();
    assertTrue(response.getBody().getResults().get(task0Id).isSuccess());
    FullTaskRecord task = taskDao.getTask(task0Id, FullTaskRecord.class);
    assertEquals(TaskStatus.WAITING.name(), task.getStatus());
    // decrementing 1 because of database rounding error.
    assertFalse(task.getNextEventTime().isAfter(ZonedDateTime.now(TwContextClockHolder.getClock()).plusSeconds(1)));
  }

  @Test
  void immediatelyResumingAllTasksWorks() {
    final UUID task0Id = transactionsHelper.withTransaction().asNew().call(() ->
        TaskTestBuilder.newTask()
            .inStatus(TaskStatus.ERROR)
            .withMaxStuckTime(ZonedDateTime.now().plusDays(1))
            .save()
            .getTaskId()
    );

    testTasksService.stopProcessing();

    ResponseEntity<ITasksManagementPort.ResumeTasksImmediatelyResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/resumeAllTasksImmediately",
        new ITasksManagementPort.ResumeAllTasksImmediatelyRequest().setTaskType("test"),
        ITasksManagementPort.ResumeTasksImmediatelyResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    assertThat(response.getBody()).isNotNull();
    assertTrue(response.getBody().getResults().get(task0Id).isSuccess());
    FullTaskRecord task = taskDao.getTask(task0Id, FullTaskRecord.class);
    // decrementing 1 because of database rounding error
    assertFalse(task.getNextEventTime().isAfter(ZonedDateTime.now(TwContextClockHolder.getClock()).plusSeconds(1)));
  }

  @Test
  void filtersErroredTasksByTypeAndSubType() {
    final List<UUID> taskIds = transactionsHelper.withTransaction().asNew().call(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2)).save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .withSubType("BAD")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("C")
          .withSubType("BAD")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withSubType("SUB")
          .save();
      var task1 = TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("B")
          .withSubType("SUB")
          .save()
          .getTaskId();
      var task2 = TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(1))
          .withType("A")
          .withSubType("SUB")
          .save()
          .getTaskId();
      return List.of(task1, task2);
    });

    ResponseEntity<GetTasksInErrorResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/getTasksInError",
        new ITasksManagementPort.GetTasksInErrorRequest().setMaxCount(10)
            .setTaskTypes(List.of("A", "B"))
            .setTaskSubTypes(List.of("SUB")),
        GetTasksInErrorResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTasksInErrorResponse tasksInErrorResponse = response.getBody();
    assertNotNull(tasksInErrorResponse);
    List<TaskInError> tasksInError = tasksInErrorResponse.getTasksInError();
    assertEquals(2, tasksInError.size());
    assertEquals(taskIds.get(0), tasksInError.get(0).getTaskVersionId().getId());
    assertEquals("B", tasksInError.get(0).getType());
    assertEquals("SUB", tasksInError.get(0).getSubType());
    assertEquals(taskIds.get(1), tasksInError.get(1).getTaskVersionId().getId());
    assertEquals("A", tasksInError.get(1).getType());
    assertEquals("SUB", tasksInError.get(1).getSubType());

  }

  @Test
  void filtersStuckTasksByTypeAndSubType() {
    testTasksService.stopProcessing();

    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2)).save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2))
          .withType("B")
          .withSubType("SUB")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2))
          .withType("A")
          .withSubType("BAD")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2))
          .withSubType("SUB")
          .save();
      return TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().minusDays(2))
          .withType("A")
          .withSubType("SUB")
          .save()
          .getTaskId();
    });

    ResponseEntity<ITasksManagementPort.GetTasksStuckResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/getTasksStuck",
        new ITasksManagementPort.GetTasksStuckRequest().setMaxCount(10)
            .setTaskTypes(List.of("A"))
            .setTaskSubTypes(List.of("SUB")),
        ITasksManagementPort.GetTasksStuckResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTasksStuckResponse stuckTasksResponse = response.getBody();
    assertNotNull(stuckTasksResponse);
    List<TaskStuck> tasksStuck = stuckTasksResponse.getTasksStuck();
    assertEquals(1, tasksStuck.size());
    assertEquals(taskId, tasksStuck.get(0).getTaskVersionId().getId());
  }

  @Test
  void filtersWaitingTasksByTypeAndSubType() {
    final UUID taskId = transactionsHelper.withTransaction().asNew().call(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2)).save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("B")
          .withSubType("SUB")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .withSubType("BAD")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withSubType("SUB")
          .save();
      return TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .withSubType("SUB")
          .save()
          .getTaskId();
    });

    ResponseEntity<GetTasksInProcessingOrWaitingResponse> response = goodEngineerTemplate().postForEntity(
        "/v1/twTasks/getTasksInProcessingOrWaiting",
        new ITasksManagementPort.GetTasksInProcessingOrWaitingRequest().setMaxCount(10)
            .setTaskTypes(List.of("A"))
            .setTaskSubTypes(List.of("SUB")),
        GetTasksInProcessingOrWaitingResponse.class
    );

    assertEquals(
        200, response.getStatusCodeValue());
    GetTasksInProcessingOrWaitingResponse waitingTasksResponse = response.getBody();
    assertNotNull(waitingTasksResponse);
    List<TaskInProcessingOrWaiting> tasksWaiting = waitingTasksResponse.getTasksInProcessingOrWaiting();
    assertEquals(1, tasksWaiting.size());
    assertEquals(taskId, tasksWaiting.get(0).getTaskVersionId().getId());
    assertEquals("A", tasksWaiting.get(0).getType());
    assertEquals("SUB", tasksWaiting.get(0).getSubType());
  }

  @Test
  void getTaskTypesWillReturnCorrectly() {
    transactionsHelper.withTransaction().asNew().run(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .withSubType("SUB-2")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .withSubType("SUB-1")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("B")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("B")
          .save();
    });

    ResponseEntity<GetTaskTypesResponse> response = goodEngineerTemplate().getForEntity(
        "/v1/twTasks/getTaskTypes",
        GetTaskTypesResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTaskTypesResponse typesResponse = response.getBody();
    assertNotNull(typesResponse);
    List<GetTaskTypesResponse.TaskType> types = typesResponse.getTypes();
    assertEquals(2, types.size());
    assertEquals("A", types.get(0).getType());
    assertEquals("B", types.get(1).getType());
    assertEquals(2, types.get(0).getSubTypes().size());
    assertEquals("SUB-1", types.get(0).getSubTypes().get(0));
    assertEquals("SUB-2", types.get(0).getSubTypes().get(1));
    assertTrue(types.get(1).getSubTypes().isEmpty());
  }

  @Test
  void getTaskTypesWillReturnFiltered() {
    transactionsHelper.withTransaction().asNew().run(() -> {
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("C")
          .withSubType("SUB-2")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("C")
          .withSubType("SUB-1")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.ERROR).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("B")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.PROCESSING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("A")
          .save();
      TaskTestBuilder.newTask().inStatus(TaskStatus.WAITING).withMaxStuckTime(ZonedDateTime.now().plusDays(2))
          .withType("C")
          .save();
    });

    ResponseEntity<GetTaskTypesResponse> response = goodEngineerTemplate().getForEntity(
        "/v1/twTasks/getTaskTypes?status=ERROR,PROCESSING",
        GetTaskTypesResponse.class
    );

    assertEquals(200, response.getStatusCodeValue());
    GetTaskTypesResponse typesResponse = response.getBody();
    assertNotNull(typesResponse);
    List<GetTaskTypesResponse.TaskType> types = typesResponse.getTypes();
    assertEquals(2, types.size());
    assertEquals("A", types.get(0).getType());
    assertEquals("B", types.get(1).getType());
    assertTrue(types.get(0).getSubTypes().isEmpty());
    assertTrue(types.get(1).getSubTypes().isEmpty());
  }

  @Test
  void getTaskTypesFailsIfEndpointIsDisabled() {
    tasksProperties.getTasksManagement().setEnableGetTaskTypes(false);
    ResponseEntity<GetTaskTypesResponse> response = goodEngineerTemplate().getForEntity(
        "/v1/twTasks/getTaskTypes?status=ERROR,PROCESSING",
        GetTaskTypesResponse.class
    );

    assertEquals(410, response.getStatusCodeValue());
    tasksProperties.getTasksManagement().setEnableGetTaskTypes(true);
  }

  private TestRestTemplate goodEngineerTemplate() {
    return testRestTemplate.withBasicAuth("goodEngineer", "q1w2e3r4");
  }

  private TestRestTemplate badEngineerTemplate() {
    return testRestTemplate.withBasicAuth("badEngineer", "q1w2e3r4");
  }

  private TestRestTemplate piiOfficerTemplate() {
    return testRestTemplate.withBasicAuth("piiOfficer", "q1w2e3r4");
  }
}
