package com.transferwise.tasks.management;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse.ResultCode;
import java.sql.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@Slf4j
public class TasksManagementPortController implements ITasksManagementPort {

  private static final int DEFAULT_MAX_COUNT = 10;

  @Autowired
  private ITasksManagementService tasksManagementService;

  @Autowired
  private ITaskDao taskDao;

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public ResponseEntity<MarkTasksAsFailedResponse> markTasksAsFailed(@RequestBody MarkTasksAsFailedRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.MarkTasksAsFailedResponse serviceResponse = tasksManagementService
          .markTasksAsFailed(new ITasksManagementService.MarkTasksAsFailedRequest()
              .setTaskVersionIds(request.getTaskVersionIds()));

      return ResponseEntity.ok(new MarkTasksAsFailedResponse().setResults(serviceResponse.getResults().entrySet().stream().collect(
          Collectors.toMap(Map.Entry::getKey,
              e -> new MarkTasksAsFailedResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess())))));
    });
  }

  @Override
  public ResponseEntity<ResumeTasksImmediatelyResponse> resumeTasksImmediately(@RequestBody ResumeTasksImmediatelyRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.ResumeTasksImmediatelyResponse serviceResponse = tasksManagementService
          .resumeTasksImmediately(new ITasksManagementService.ResumeTasksImmediatelyRequest()
              .setTaskVersionIds(request.getTaskVersionIds()));

      return ResponseEntity.ok(new ResumeTasksImmediatelyResponse().setResults(serviceResponse.getResults().entrySet().stream().collect(
          Collectors
              .toMap(Map.Entry::getKey,
                  e -> new ResumeTasksImmediatelyResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess())))));
    });
  }

  @Override
  public ResponseEntity<ResumeTasksImmediatelyResponse> resumeAllTasksImmediately(@RequestBody ResumeAllTasksImmediatelyRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.ResumeTasksImmediatelyResponse serviceResponse = tasksManagementService
          .resumeAllTasksImmediately(new ITasksManagementService.ResumeAllTasksImmediatelyRequest()
              .setTaskType(request.getTaskType())
              .setMaxCount(request.getMaxCount() != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

      return ResponseEntity.ok(new ResumeTasksImmediatelyResponse().setResults(serviceResponse.getResults().entrySet().stream().collect(
          Collectors
              .toMap(Map.Entry::getKey,
                  e -> new ResumeTasksImmediatelyResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess())))));
    });
  }

  @Override
  public ResponseEntity<GetTasksInErrorResponse> getTasksInError(@RequestBody(required = false) GetTasksInErrorRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.GetTasksInErrorResponse serviceResponse = tasksManagementService
          .getTasksInError(new ITasksManagementService.GetTasksInErrorRequest()
              .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

      return ResponseEntity.ok(new GetTasksInErrorResponse().setTasksInError(serviceResponse.getTasksInError().stream().map(taskInError ->
          new GetTasksInErrorResponse.TaskInError()
              .setErrorTime(taskInError.getErrorTime())
              .setTaskVersionId(taskInError.getTaskVersionId())
              .setType(taskInError.getType()).setSubType(taskInError.getSubType())
      ).collect(Collectors.toList())));
    });
  }

  @Override
  public ResponseEntity<TaskWithoutData> getTask(@PathVariable final String taskId) {
    return callWithAuthentication(() -> {
      FullTaskRecord task = taskDao.getTask(UUID.fromString(taskId), FullTaskRecord.class);
      return ResponseEntity.ok(new TaskWithoutData()
          .setId(taskId)
          .setVersion(task.getVersion())
          .setStatus(task.getStatus())
          .setType(task.getType())
          .setSubType(task.getSubType())
          .setNextEventTime(Date.from(task.getNextEventTime().toInstant()))
          .setStateTime(Date.from(task.getStateTime().toInstant()))
          .setPriority(task.getPriority())
          .setProcessingTriesCount(task.getProcessingTriesCount())
          .setProcessingClientId(task.getProcessingClientId()));
    });
  }

  @Override
  public ResponseEntity<GetTaskDataResponse> getTaskData(UUID taskId) {
    return callWithAuthentication(tasksProperties.getTasksManagement().getViewTaskDataRoles(),
        (auth) -> {
          GetTaskDataResponse response = new GetTaskDataResponse();

          final FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
          if (task == null) {
            response.setResultCode(ResultCode.NOT_FOUND);
            log.info("User '{}' tried to fetch payload for task '{}', but it was not found.", auth.getName(), taskId);
          } else {
            response.setResultCode(ResultCode.SUCCESS).setData(task.getData());
            log.info("User '{}' fetched payload for task '{}' of type '{}'.", auth.getName(), new TaskVersionId(task.getId(), task.getVersion()),
                task.getType());
          }
          return ResponseEntity.ok(response);
        });
  }

  @Override
  public ResponseEntity<GetTasksInProcessingOrWaitingResponse> getTasksInProcessingOrWaiting(
      @RequestBody(required = false) GetTasksInProcessingOrWaitingRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.GetTasksInProcessingOrWaitingResponse serviceResponse = tasksManagementService
          .getTasksInProcessingOrWaiting(new ITasksManagementService.GetTasksInProcessingOrWaitingRequest()
              .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

      return ResponseEntity.ok(new GetTasksInProcessingOrWaitingResponse()
          .setTasksInProcessingOrWaiting(
              serviceResponse.getTasksInProcessingOrWaiting()
                  .stream()
                  .map(task ->
                      new GetTasksInProcessingOrWaitingResponse.TaskInProcessingOrWaiting()
                          .setTaskVersionId(task.getTaskVersionId())
                          .setStatus(task.getStatus())
                          .setStateTime(task.getStateTime())
                          .setType(task.getType())
                          .setSubType(task.getSubType())
                  )
                  .collect(Collectors.toList())
          ));
    });
  }

  @Override
  public ResponseEntity<GetTasksByIdResponse> getTasksById(@RequestBody GetTasksByIdRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.GetTasksByIdRequest tasksByIdRequest = new ITasksManagementService.GetTasksByIdRequest()
          .setTaskIds(request.getTaskIds());

      ITasksManagementService.GetTasksByIdResponse tasksById = tasksManagementService.getTasksById(tasksByIdRequest);
      return ResponseEntity.ok(new GetTasksByIdResponse().setTaskList(
          tasksById.getTasks().stream().map(task ->
              new GetTasksByIdResponse.Task()
                  .setTaskVersionId(task.getTaskVersionId())
                  .setType(task.getType())
                  .setSubType(task.getSubType())
                  .setStatus(task.getStatus())
                  .setStateTime(task.getStateTime())
          ).collect(Collectors.toList()))
      );
    });
  }

  @Override
  public ResponseEntity<GetTasksStuckResponse> getTasksStuck(@RequestBody(required = false) GetTasksStuckRequest request) {
    return callWithAuthentication(() -> {
      ITasksManagementService.GetTasksStuckResponse serviceResponse = tasksManagementService
          .getTasksStuck(new ITasksManagementService.GetTasksStuckRequest()
              .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

      return ResponseEntity.ok(new GetTasksStuckResponse().setTasksStuck(serviceResponse.getTasksStuck().stream().map(taskStuck ->
          new GetTasksStuckResponse.TaskStuck().setStuckTime(taskStuck.getStuckTime())
              .setTaskVersionId(taskStuck.getTaskVersionId())).collect(Collectors.toList())));
    });
  }

  protected <T> T callWithAuthentication(Supplier<T> supplier) {
    return callWithAuthentication(tasksProperties.getTasksManagement().getRoles(), (auth) -> supplier.get());
  }

  @SuppressWarnings("unchecked")
  protected <T> T callWithAuthentication(Set<String> roles, Function<Authentication, T> fun) {
    final Authentication auth = getAuthenticationIfAllowed(roles);

    if (auth == null) {
      return (T) ResponseEntity.status(HttpStatus.FORBIDDEN).build();
    }

    return fun.apply(auth);
  }

  protected Authentication getAuthenticationIfAllowed(Set<String> roles) {
    Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    if (authentication == null) {
      throw new AuthenticationServiceException("Service has no authentication configured");
    }

    // We consider this as user indicated, that he wants to rely on his service's security system.
    if (CollectionUtils.isEmpty(roles)) {
      return authentication;
    }

    for (GrantedAuthority authority : authentication.getAuthorities()) {
      if (roles.contains(StringUtils.upperCase(authority.getAuthority()))) {
        return authentication;
      }
    }

    return null;
  }
}
