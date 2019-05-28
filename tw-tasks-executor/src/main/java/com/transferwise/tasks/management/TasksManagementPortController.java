package com.transferwise.tasks.management;

import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;

import java.sql.Date;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Controller
public class TasksManagementPortController implements ITasksManagementPort {

    private static final int DEFAULT_MAX_COUNT = 10;

    @Autowired
    private ITasksManagementService tasksManagementService;

    @Autowired
    private ITaskDao taskDao;

    @Override
    public MarkTasksAsFailedResponse markTasksAsFailed(@RequestBody MarkTasksAsFailedRequest request) {
        ITasksManagementService.MarkTasksAsFailedResponse sResponse = tasksManagementService.markTasksAsFailed(new ITasksManagementService.MarkTasksAsFailedRequest()
            .setTaskVersionIds(request.getTaskVersionIds()));

        return new MarkTasksAsFailedResponse().setResults(sResponse.getResults().entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, e -> new MarkTasksAsFailedResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess()))));
    }

    @Override
    public ResumeTasksImmediatelyResponse resumeTasksImmediately(@RequestBody ResumeTasksImmediatelyRequest request) {
        ITasksManagementService.ResumeTasksImmediatelyResponse sResponse = tasksManagementService
            .resumeTasksImmediately(new ITasksManagementService.ResumeTasksImmediatelyRequest()
                .setTaskVersionIds(request.getTaskVersionIds()));

        return new ResumeTasksImmediatelyResponse().setResults(sResponse.getResults().entrySet().stream().collect(
            Collectors
                .toMap(Map.Entry::getKey, e -> new ResumeTasksImmediatelyResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess()))));
    }

    @Override
    public ResumeTasksImmediatelyResponse resumeAllTasksImmediately(@RequestBody ResumeAllTasksImmediatelyRequest request) {
        ITasksManagementService.ResumeTasksImmediatelyResponse sResponse = tasksManagementService
            .resumeAllTasksImmediately(new ITasksManagementService.ResumeAllTasksImmediatelyRequest()
                .setTaskType(request.getTaskType())
                .setMaxCount(request.getMaxCount() != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

        return new ResumeTasksImmediatelyResponse().setResults(sResponse.getResults().entrySet().stream().collect(
            Collectors
                .toMap(Map.Entry::getKey, e -> new ResumeTasksImmediatelyResponse.Result().setMessage(e.getValue().getMessage()).setSuccess(e.getValue().isSuccess()))));
    }

    @Override
    public GetTasksInErrorResponse getTasksInError(@RequestBody(required = false) GetTasksInErrorRequest request) {
        ITasksManagementService.GetTasksInErrorResponse sResponse = tasksManagementService
            .getTasksInError(new ITasksManagementService.GetTasksInErrorRequest()
                .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

        return new GetTasksInErrorResponse().setTasksInError(sResponse.getTasksInError().stream().map(taskInError ->
                new GetTasksInErrorResponse.TaskInError()
                    .setErrorTime(taskInError.getErrorTime())
                    .setTaskVersionId(taskInError.getTaskVersionId())
                    .setType(taskInError.getType()).setSubType(taskInError.getSubType())
                                                                                                     ).collect(Collectors.toList()));
    }

    @Override
    public TaskWithoutData getTask(@PathVariable final String taskId) {
        FullTaskRecord task = taskDao.getTask(UUID.fromString(taskId), FullTaskRecord.class);
        return new TaskWithoutData()
            .setId(taskId)
            .setVersion(task.getVersion())
            .setStatus(task.getStatus())
            .setType(task.getType())
            .setSubType(task.getSubType())
            .setNextEventTime(Date.from(task.getNextEventTime().toInstant()))
            .setStateTime(Date.from(task.getStateTime().toInstant()))
            .setPriority(task.getPriority())
            .setProcessingTriesCount(task.getProcessingTriesCount())
            .setProcessingClientId(task.getProcessingClientId());
    }

    @Override
    public GetTasksInProcessingOrWaitingResponse getTasksInProcessingOrWaiting(@RequestBody(required = false) GetTasksInProcessingOrWaitingRequest request) {
        ITasksManagementService.GetTasksInProcessingOrWaitingResponse sResponse = tasksManagementService
            .getTasksInProcessingOrWaiting(new ITasksManagementService.GetTasksInProcessingOrWaitingRequest()
                .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

        return new GetTasksInProcessingOrWaitingResponse()
            .setTasksInProcessingOrWaiting(
                sResponse.getTasksInProcessingOrWaiting()
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
                                          );
    }

    @Override
    public GetTasksByIdResponse getTasksById(@RequestBody GetTasksByIdRequest request) {
        ITasksManagementService.GetTasksByIdRequest tasksByIdRequest = new ITasksManagementService.GetTasksByIdRequest()
            .setTaskIds(request.getTaskIds());

        ITasksManagementService.GetTasksByIdResponse tasksById = tasksManagementService.getTasksById(tasksByIdRequest);
        return new GetTasksByIdResponse().setTaskList(
            tasksById.getTasks().stream().map(task ->
                    new GetTasksByIdResponse.Task()
                        .setTaskVersionId(task.getTaskVersionId())
                        .setType(task.getType())
                        .setSubType(task.getSubType())
                        .setStatus(task.getStatus())
                        .setStateTime(task.getStateTime())
                                             ).collect(Collectors.toList())
                                                     );
    }

    @Override
    public GetTasksStuckResponse getTasksStuck(@RequestBody(required = false) GetTasksStuckRequest request) {
        ITasksManagementService.GetTasksStuckResponse sResponse = tasksManagementService
            .getTasksStuck(new ITasksManagementService.GetTasksStuckRequest()
                .setMaxCount(request != null ? request.getMaxCount() : DEFAULT_MAX_COUNT));

        return new GetTasksStuckResponse().setTasksStuck(sResponse.getTasksStuck().stream().map(taskStuck ->
            new GetTasksStuckResponse.TaskStuck().setStuckTime(taskStuck.getStuckTime())
                .setTaskVersionId(taskStuck.getTaskVersionId())).collect(Collectors.toList()));
    }
}
