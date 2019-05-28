package com.transferwise.tasks.test;

import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;

import java.util.List;
import java.util.function.Predicate;

public interface ITestTasksService extends ITasksService {
    List<Task> getFinishedTasks(String type, String subType);

    void reset(); // beware, there are cron-job type tasks that should not be deleted

    void resetAndDeleteTasksWithTypes(String... types); // use for cleanup after a test case.

    List<Task> getWaitingTasks(String type, String subType);

    void cleanFinishedTasks(String type, String subType);

    void interceptNewTasks(Predicate<AddTaskRequest> predicate);

    TaskTrackerHandler startTrackingAddTasks(Predicate<AddTaskRequest> predicate);

    void stopTracking(TaskTrackerHandler handler);

    List<AddTaskRequest> getInterceptedNewTasks();

    List<Task> getTasks(String type, String subType, TaskStatus... statuses);
}
