package com.transferwise.tasks.management;

import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.entrypoints.IMdcService;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse.ResultCode;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskWithoutDataResponse;
import com.transferwise.tasks.management.ITasksManagementService.GetTaskDataRequest.ContentFormat;
import com.transferwise.tasks.management.dao.IManagementTaskDao;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask1;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask2;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask3;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTaskType;
import com.transferwise.tasks.utils.LogUtils;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

/**
 * Intended to be used only for management interfaces and not from applications` code. All the operations are not optimized for frequent use.
 *
 * <p>Normally, applications should only use {@link com.transferwise.tasks.TasksService}
 */
@Slf4j
public class TasksManagementService implements ITasksManagementService {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private IManagementTaskDao managementTaskDao;
  @Autowired
  private IMdcService mdcService;
  @Autowired
  private IEntryPointsService entryPointsHelper;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  @Override
  @EntryPoint(usesExisting = true)
  @Transactional(rollbackFor = Exception.class)
  public MarkTasksAsFailedResponse markTasksAsFailed(MarkTasksAsFailedRequest request) {
    return entryPointsHelper.continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.MARK_AS_FAILED,
        () -> {
          MarkTasksAsFailedResponse response = new MarkTasksAsFailedResponse();

          for (TaskVersionId taskVersionId : request.getTaskVersionIds()) {
            mdcService.put(taskVersionId.getId(), taskVersionId.getVersion());
            BaseTask1 task = taskDao.getTask(taskVersionId.getId(), BaseTask1.class);
            mdcService.put(task);

            boolean succeeded = taskDao.setStatus(taskVersionId.getId(), TaskStatus.FAILED, taskVersionId.getVersion());
            log.info("Marking of task '" + taskVersionId.getId() + "' as FAILED " + (succeeded ? " succeeded" : "failed") + ".");
            if (succeeded) {
              coreMetricsTemplate.registerTaskMarkedAsFailed(null, task.getType());
            } else {
              coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.FAILED);
            }
            response.getResults().put(taskVersionId.getId(), new MarkTasksAsFailedResponse.Result().setSuccess(succeeded));
          }
          return response;
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  @Transactional(rollbackFor = Exception.class)
  public ResumeTasksImmediatelyResponse resumeTasksImmediately(ResumeTasksImmediatelyRequest request) {
    return entryPointsHelper.continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.RESUME_IMMEDIATELY,
        () -> {
          ResumeTasksImmediatelyResponse response = new ResumeTasksImmediatelyResponse();

          for (TaskVersionId taskVersionId : request.getTaskVersionIds()) {
            if (taskVersionId == null || taskVersionId.getId() == null) {
              continue;
            }
            mdcService.put(taskVersionId.getId(), taskVersionId.getVersion());
            BaseTask1 task = taskDao.getTask(taskVersionId.getId(), BaseTask1.class);
            mdcService.put(task);
            if (task == null) {
              response.getResults().put(taskVersionId.getId(), new ResumeTasksImmediatelyResponse.Result()
                  .setMessage("Task with given id not not found.").setSuccess(false));
              log.warn("Task " + LogUtils.asParameter(taskVersionId) + " was tried to immediately resumed, but it does not exist.");
            } else {
              boolean succeeded = managementTaskDao.scheduleTaskForImmediateExecution(taskVersionId.getId(), taskVersionId.getVersion());
              log.info("Marking task " + LogUtils.asParameter(taskVersionId) + " in status '" + task
                  .getStatus() + "' to be immediately resumed " + (succeeded ? " succeeded" : "failed") + ".");
              if (succeeded) {
                coreMetricsTemplate.registerTaskResuming(null, task.getType());
              } else {
                coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.WAITING);
              }
              response.getResults().put(taskVersionId.getId(), new ResumeTasksImmediatelyResponse.Result()
                  .setSuccess(succeeded));
            }
          }

          return response;
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  @Transactional(rollbackFor = Exception.class)
  public ResumeTasksImmediatelyResponse resumeAllTasksImmediately(ResumeAllTasksImmediatelyRequest request) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.RESUME_ALL_IMMEDIATELY, () -> {
          ResumeTasksImmediatelyResponse response = new ResumeTasksImmediatelyResponse();

          if (StringUtils.isEmpty(request.getTaskType())) {
            return response;
          }

          List<DaoTask1> tasksInError = managementTaskDao.getTasksInErrorStatus(request.getMaxCount(), List.of(request.getTaskType()), null);
          List<TaskVersionId> taskVersionIdsToResume = tasksInError.stream()
              .filter(t -> t.getType().equals(request.getTaskType()))
              .map(t -> new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
              .collect(Collectors.toList());

          return resumeTasksImmediately(new ResumeTasksImmediatelyRequest().setTaskVersionIds(taskVersionIdsToResume));
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTasksInErrorResponse getTasksInError(GetTasksInErrorRequest request) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASKS_IN_ERROR, () -> {
          List<DaoTask1> tasks = managementTaskDao.getTasksInErrorStatus(request.getMaxCount(), request.getTaskTypes(), request.getTaskSubTypes());

          return new GetTasksInErrorResponse().setTasksInError(
              tasks.stream().map(t -> new GetTasksInErrorResponse.TaskInError()
                  .setErrorTime(t.getStateTime().toInstant())
                  .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
                  .setType(t.getType())
                  .setSubType(t.getSubType()))
                  .collect(Collectors.toList()));
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTasksStuckResponse getTasksStuck(GetTasksStuckRequest request) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASKS_STUCK, () -> {
          List<DaoTask2> tasks = managementTaskDao.getStuckTasks(request.getMaxCount(), request.getTaskTypes(), request.getTaskSubTypes(),
              request.getDelta() == null ? Duration.ofSeconds(10) : request.getDelta());

          return new GetTasksStuckResponse().setTasksStuck(
              tasks.stream().map(t -> new GetTasksStuckResponse.TaskStuck()
                  .setStuckTime(t.getNextEventTime().toInstant())
                  .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion())))
                  .collect(Collectors.toList()));
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTasksInProcessingOrWaitingResponse getTasksInProcessingOrWaiting(GetTasksInProcessingOrWaitingRequest request) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASKS_IN_PROCESSING_OR_WAITING,
            () -> {
              List<DaoTask3> tasks = managementTaskDao.getTasksInProcessingOrWaitingStatus(
                  request.getMaxCount(), request.getTaskTypes(), request.getTaskSubTypes());
              return new GetTasksInProcessingOrWaitingResponse().setTasksInProcessingOrWaiting(
                  tasks.stream().map(t -> new GetTasksInProcessingOrWaitingResponse.TaskInProcessingOrWaiting()
                      .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
                      .setType(t.getType())
                      .setSubType(t.getSubType())
                      .setStatus(t.getStatus())
                      .setStateTime(t.getStateTime().toInstant()))
                      .collect(Collectors.toList()));
            });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTasksByIdResponse getTasksById(GetTasksByIdRequest request) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASKS_BY_ID, () -> {
          List<FullTaskRecord> tasks = managementTaskDao.getTasks(request.getTaskIds());
          return new GetTasksByIdResponse().setTasks(
              tasks.stream().map(t -> new GetTasksByIdResponse.Task()
                  .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
                  .setType(t.getType())
                  .setSubType(t.getSubType())
                  .setStatus(t.getStatus())
                  .setStateTime(t.getStateTime().toInstant())
              )
                  .collect(Collectors.toList())
          );
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTaskWithoutDataResponse getTaskWithoutData(UUID taskId) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASK_WITHOUT_DATA,
            () -> {
              mdcService.put(taskId);
              FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
              mdcService.put(task);
              return new GetTaskWithoutDataResponse()
                  .setId(taskId)
                  .setVersion(task.getVersion())
                  .setStatus(task.getStatus())
                  .setType(task.getType())
                  .setSubType(task.getSubType())
                  .setNextEventTime(task.getNextEventTime().toInstant())
                  .setStateTime(task.getStateTime().toInstant())
                  .setPriority(task.getPriority())
                  .setProcessingTriesCount(task.getProcessingTriesCount())
                  .setProcessingClientId(task.getProcessingClientId());
            });
  }

  @Override
  @EntryPoint(usesExisting = true)
  public GetTaskDataResponse getTaskData(GetTaskDataRequest request) {
    return entryPointsHelper.continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASK_DATA,
        () -> {
          UUID taskId = request.getTaskId();
          mdcService.put(taskId);
          GetTaskDataResponse response = new GetTaskDataResponse();

          final FullTaskRecord task = taskDao.getTask(taskId, FullTaskRecord.class);
          if (task == null) {
            response.setResultCode(ResultCode.NOT_FOUND);
          } else {
            mdcService.put(task);
            response.setVersion(task.getVersion());
            response.setType(task.getType());

            response.setResultCode(ResultCode.SUCCESS);
            if (task.getData() != null) {
              if (request.getContentFormat() == ContentFormat.BASE64) {
                response.setData(Base64.getEncoder().encodeToString(task.getData()));
              } else {
                response.setData(new String(task.getData(), StandardCharsets.UTF_8));
              }
            }
          }
          return response;
        });
  }

  @Override
  public GetTaskTypesResponse getTaskTypes(List<String> status) {
    return entryPointsHelper
        .continueOrCreate(ManagementEntryPointGroups.TW_TASKS_MANAGEMENT, ManagementEntryPointNames.GET_TASKS_TYPES, () -> {
          List<DaoTaskType> types = managementTaskDao.getTaskTypes(status);
          return new GetTaskTypesResponse().setTypes(
              types.stream().map(t -> new GetTaskTypesResponse.TaskType().setType(t.getType()).setSubTypes(t.getSubTypes()))
                  .collect(Collectors.toList()));
        });
  }
}
