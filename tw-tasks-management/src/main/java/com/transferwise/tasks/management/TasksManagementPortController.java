package com.transferwise.tasks.management;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.TasksProperties.TasksManagement.TypeSpecificTaskManagement;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse.ResultCode;
import com.transferwise.tasks.management.ITasksManagementService.GetTaskDataRequest;
import com.transferwise.tasks.management.ITasksManagementService.GetTaskDataRequest.ContentFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationServiceException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Slf4j
public class TasksManagementPortController implements ITasksManagementPort, InitializingBean {

  private static final int DEFAULT_MAX_COUNT = 10;

  @Autowired
  private ITasksManagementService tasksManagementService;

  @Autowired
  private TasksProperties tasksProperties;

  private Map<String, Set<String>> allowedRolesByTaskType;

  @Override
  public void afterPropertiesSet() {
    allowedRolesByTaskType = tasksProperties.getTasksManagement()
        .getTypeSpecific()
        .stream()
        .collect(
            Collectors.toMap(
                TypeSpecificTaskManagement::getTaskType,
                TypeSpecificTaskManagement::getViewTaskDataRoles));
  }

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
              .setTaskTypes(request != null ? request.getTaskTypes() : null)
              .setTaskSubTypes(request != null ? request.getTaskSubTypes() : null)
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
  public ResponseEntity<GetTaskWithoutDataResponse> getTaskWithoutData(@PathVariable final UUID taskId) {
    return callWithAuthentication(() -> ResponseEntity.ok(tasksManagementService.getTaskWithoutData(taskId)));
  }

  @Override
  public ResponseEntity<GetTaskDataResponse> getTaskData(UUID taskId, String format) {
    GetTaskDataRequest request = new GetTaskDataRequest().setTaskId(taskId).setContentFormat(ContentFormat.of(format));
    GetTaskDataResponse response = tasksManagementService.getTaskData(request);
    Set<String> allowedRoles = getAllowedRoles(response);

    return callWithAuthentication(allowedRoles,
        (auth) -> {

          if (response.getResultCode() == ResultCode.NOT_FOUND) {
            log.info("User '{}' tried to fetch payload for task '{}', but it was not found.", auth.getName(), taskId);
          } else {
            log.info("User '{}' fetched payload for task '{}' of type '{}'.", auth.getName(), new TaskVersionId(taskId, response.getVersion()),
                response.getType());
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
              .setTaskTypes(request != null ? request.getTaskTypes() : null)
              .setTaskSubTypes(request != null ? request.getTaskSubTypes() : null)
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
              .setTaskTypes(request != null ? request.getTaskTypes() : null)
              .setTaskSubTypes(request != null ? request.getTaskSubTypes() : null)
              .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

      return ResponseEntity.ok(new GetTasksStuckResponse().setTasksStuck(serviceResponse.getTasksStuck().stream().map(taskStuck ->
          new GetTasksStuckResponse.TaskStuck().setStuckTime(taskStuck.getStuckTime())
              .setTaskVersionId(taskStuck.getTaskVersionId())).collect(Collectors.toList())));
    });
  }

  @Override
  public ResponseEntity<GetTaskTypesResponse> getTaskTypes(@RequestParam(name = "status", required = false) List<String> status) {
    if (!tasksProperties.getTasksManagement().isEnableGetTaskTypes()) {
      return ResponseEntity.status(410).build();
    }
    return callWithAuthentication(() -> {
      ITasksManagementService.GetTaskTypesResponse serviceResponse = tasksManagementService.getTaskTypes(status);

      return ResponseEntity.ok(new GetTaskTypesResponse().setTypes(serviceResponse.getTypes().stream().map(type ->
          new GetTaskTypesResponse.TaskType().setType(type.getType()).setSubTypes(type.getSubTypes())).collect(Collectors.toList())));
    });
  }

  protected <T> T callWithAuthentication(Supplier<T> supplier) {
    return callWithAuthentication(tasksProperties.getTasksManagement().getRoles(), (auth) -> supplier.get());
  }

  @SuppressWarnings("unchecked")
  protected <T> T callWithAuthentication(Set<String> roles, Function<Authentication, T> fun) {
    final Authentication auth = getAuthenticationIfAllowed(roles);

    if (auth == null) {
      return (T) ResponseEntity.status(403).build();
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

  private Set<String> getAllowedRoles(GetTaskDataResponse taskData) {
    return allowedRolesByTaskType.getOrDefault(
        taskData.getType(),
        tasksProperties.getTasksManagement().getViewTaskDataRoles());
  }
}
