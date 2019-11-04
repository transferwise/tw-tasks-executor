package com.transferwise.tasks.management;

import com.newrelic.api.agent.Trace;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.mdc.MdcContext;
import com.transferwise.tasks.utils.LogUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Intended to be used only for management interfaces and not from applications` code.
 * All the operations are not optimized for frequent use.
 * <p>
 * Normally, applications should only use {@link com.transferwise.tasks.TasksService}
 */
@Slf4j
public class TasksManagementService implements ITasksManagementService {
    @Autowired
    private ITaskDao taskDao;

    @Autowired
    private TasksProperties tasksProperties;

    @Autowired
    private IMeterHelper meterHelper;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public MarkTasksAsFailedResponse markTasksAsFailed(MarkTasksAsFailedRequest request) {
        MarkTasksAsFailedResponse response = new MarkTasksAsFailedResponse();

        for (TaskVersionId taskVersionId : request.getTaskVersionIds()) {
            MdcContext.with(() -> {
                MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), taskVersionId);
                BaseTask1 task = taskDao.getTask(taskVersionId.getId(), BaseTask1.class);

                boolean succeeded = taskDao.setStatus(taskVersionId.getId(), TaskStatus.FAILED, taskVersionId.getVersion());
                log.info("Marking of task '" + taskVersionId.getId() + "' as FAILED " + (succeeded ? " succeeded" : "failed") + ".");
                if (succeeded) {
                    meterHelper.registerTaskMarkedAsFailed(null, task.getType());
                } else {
                    meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.FAILED);
                }
                response.getResults().put(taskVersionId.getId(), new MarkTasksAsFailedResponse.Result().
                    setSuccess(succeeded));
            });
        }
        return response;
    }

    @Override
    @Trace
    @Transactional(rollbackFor = Exception.class)
    public ResumeTasksImmediatelyResponse resumeTasksImmediately(ResumeTasksImmediatelyRequest request) {
        ResumeTasksImmediatelyResponse response = new ResumeTasksImmediatelyResponse();

        for (TaskVersionId taskVersionId : request.getTaskVersionIds()) {
            if (taskVersionId == null || taskVersionId.getId() == null) {
                continue;
            }
            MdcContext.with(() -> {
                MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), taskVersionId);
                BaseTask1 task = taskDao.getTask(taskVersionId.getId(), BaseTask1.class);
                if (task == null) {
                    response.getResults().put(taskVersionId.getId(), new ResumeTasksImmediatelyResponse.Result()
                        .setMessage("Task with given id not not found.").setSuccess(false));
                    log.warn("Task " + LogUtils.asParameter(taskVersionId) + " was tried to immediately resumed, but it does not exist.");
                } else {
                    boolean succeeded = taskDao.scheduleTaskForImmediateExecution(taskVersionId.getId(), taskVersionId.getVersion());
                    log.info("Marking task " + LogUtils.asParameter(taskVersionId) + " in status '" + task
                        .getStatus() + "' to be immediately resumed " + (succeeded ? " succeeded" : "failed") + ".");
                    if (succeeded) {
                        meterHelper.registerTaskResuming(null, task.getType());
                    } else {
                        meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.WAITING);
                    }
                    response.getResults().put(taskVersionId.getId(), new ResumeTasksImmediatelyResponse.Result().
                        setSuccess(succeeded));
                }
            });
        }

        return response;
    }

    @Override
    @Trace
    @Transactional(rollbackFor = Exception.class)
    public ResumeTasksImmediatelyResponse resumeAllTasksImmediately(ResumeAllTasksImmediatelyRequest request) {
        ResumeTasksImmediatelyResponse response = new ResumeTasksImmediatelyResponse();

        if (StringUtils.isEmpty(request.getTaskType())) {
            return response;
        }

        List<ITaskDao.DaoTask1> tasksInError = taskDao.getTasksInErrorStatus(request.getMaxCount());
        List<TaskVersionId> taskVersionIdsToResume = tasksInError.stream()
            .filter(t -> t.getType().equals(request.getTaskType()))
            .map(t -> new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
            .collect(Collectors.toList());

        return resumeTasksImmediately(new ResumeTasksImmediatelyRequest().setTaskVersionIds(taskVersionIdsToResume));
    }

    @Override
    @Trace
    public GetTasksInErrorResponse getTasksInError(GetTasksInErrorRequest request) {
        List<ITaskDao.DaoTask1> tasks = taskDao.getTasksInErrorStatus(request.getMaxCount());

        return new GetTasksInErrorResponse().setTasksInError(
            tasks.stream().map(t -> new GetTasksInErrorResponse.TaskInError()
                .setErrorTime(t.getStateTime().toInstant())
                .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
                .setType(t.getType())
                .setSubType(t.getSubType()))
                .collect(Collectors.toList()));
    }

    @Override
    @Trace
    @SuppressWarnings("checkstyle:magicnumber")
    public GetTasksStuckResponse getTasksStuck(GetTasksStuckRequest request) {
        List<ITaskDao.DaoTask2> tasks = taskDao.getStuckTasks(request.getMaxCount(), request.getDelta() == null ?
            Duration.ofSeconds(10) : request.getDelta());

        return new GetTasksStuckResponse().setTasksStuck(
            tasks.stream().map(t -> new GetTasksStuckResponse.TaskStuck()
                .setStuckTime(t.getNextEventTime().toInstant())
                .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion())))
                .collect(Collectors.toList()));
    }

    @Override
    public GetTasksInProcessingOrWaitingResponse getTasksInProcessingOrWaiting(GetTasksInProcessingOrWaitingRequest request) {
        List<ITaskDao.DaoTask3> tasks = taskDao.getTasksInProcessingOrWaitingStatus(request.getMaxCount());
        return new GetTasksInProcessingOrWaitingResponse().setTasksInProcessingOrWaiting(
            tasks.stream().map(t -> new GetTasksInProcessingOrWaitingResponse.TaskInProcessingOrWaiting()
                .setTaskVersionId(new TaskVersionId().setId(t.getId()).setVersion(t.getVersion()))
                .setType(t.getType())
                .setSubType(t.getSubType())
                .setStatus(t.getStatus())
                .setStateTime(t.getStateTime().toInstant()))
                .collect(Collectors.toList()));
    }

    @Override
    public GetTasksByIdResponse getTasksById(GetTasksByIdRequest request) {
        List<FullTaskRecord> tasks = taskDao.getTasks(request.getTaskIds());
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
    }
}
