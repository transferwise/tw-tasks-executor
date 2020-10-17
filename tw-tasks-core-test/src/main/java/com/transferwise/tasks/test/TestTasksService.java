package com.transferwise.tasks.test;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.TasksService;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.stucktasks.ITasksResumer;
import com.transferwise.tasks.test.dao.TestTaskDao;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class TestTasksService extends TasksService implements ITestTasksService {

  private Predicate<AddTaskRequest> newTaskInterceptPredicate;
  private final Map<TaskTrackerHandler, Predicate<AddTaskRequest>> taskAdditionTrackers = new HashMap<>();
  private final List<AddTaskRequest> interceptedNewTasks = new ArrayList<>();

  @Autowired
  private TestTaskDao testTaskDao;

  @Autowired
  private IBucketsManager bucketsManager;

  @Autowired
  private ITasksResumer tasksResumer;

  @Override
  public List<Task> getFinishedTasks(String type, String subType) {
    return testTaskDao.findTasksByTypeSubTypeAndStatus(type, subType, TaskStatus.DONE);
  }

  @Override
  public void reset() {
    testTaskDao.deleteAllTasks();
    newTaskInterceptPredicate = null;
    interceptedNewTasks.clear();
    taskAdditionTrackers.clear();
  }

  @Override
  public void resetAndDeleteTasksWithTypes(String... types) {
    Arrays.stream(types).forEach(type -> testTaskDao.deleteTasks(type, null));
    newTaskInterceptPredicate = null;
    interceptedNewTasks.clear();
  }

  @Override
  public List<Task> getWaitingTasks(String type, String subType) {
    return testTaskDao.findTasksByTypeSubTypeAndStatus(type, subType, TaskStatus.WAITING);
  }

  @Override
  public List<Task> getTasks(String type, String subType, TaskStatus... statuses) {
    return testTaskDao.findTasksByTypeSubTypeAndStatus(type, subType, statuses);
  }

  @Override
  public void stopProcessing() {
    tasksResumer.pauseProcessing();
    List<Future<Void>> futures = new ArrayList<>();
    for (String bucketId : bucketsManager.getBucketIds()) {
      futures.add(stopTasksProcessing(bucketId));
    }

    for (Future<Void> future : futures) {
      ExceptionUtils.doUnchecked(() -> {
        future.get();
      });
    }
  }

  @Override
  public void resumeProcessing() {
    tasksResumer.resumeProcessing();
    for (String bucketId : bucketsManager.getBucketIds()) {
      if (!Boolean.FALSE.equals(bucketsManager.getBucketProperties(bucketId).getAutoStartProcessing())) {
        startTasksProcessing(bucketId);
      }
    }
  }

  @Override
  public void cleanFinishedTasks(String type, String subType) {
    testTaskDao.deleteTasks(type, subType, TaskStatus.DONE);
  }

  @Override
  public void interceptNewTasks(Predicate<AddTaskRequest> predicate) {
    this.newTaskInterceptPredicate = predicate;
  }

  @Override
  public TaskTrackerHandler startTrackingAddTasks(Predicate<AddTaskRequest> predicate) {
    TaskTrackerHandler taskTrackerHandler = new TaskTrackerHandler();
    taskAdditionTrackers.put(taskTrackerHandler, predicate);
    return taskTrackerHandler;
  }

  @Override
  public void stopTracking(TaskTrackerHandler handler) {
    taskAdditionTrackers.remove(handler);
  }

  @Override
  public List<AddTaskRequest> getInterceptedNewTasks() {
    return interceptedNewTasks;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public AddTaskResponse addTask(AddTaskRequest request) {
    track(request);
    if (newTaskInterceptPredicate != null && newTaskInterceptPredicate.test(request)) {
      interceptedNewTasks.add(request);
      UUID taskId = request.getTaskId() == null ? UuidUtils.generatePrefixCombUuid() : request.getTaskId();
      log.info("Intercepted task '" + taskId + "' with type '" + request.getType() + "'.");
      return new AddTaskResponse().setResult(AddTaskResponse.Result.OK).setTaskId(taskId);
    } else {
      return super.addTask(request);
    }
  }

  private void track(AddTaskRequest request) {
    taskAdditionTrackers
        .entrySet()
        .stream()
        .filter(it -> it.getValue().test(request))
        .forEach(it -> it.getKey().track(request));
  }
}
