package com.transferwise.tasks.testapp;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.TasksProcessingState;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class ManualStartIntTest extends BaseIntTest {

  private static final String BUCKET_ID = "manualStart";

  private TestClock testClock;

  @Autowired
  private ITaskDao taskDao;

  @BeforeEach
  void setup() {
    testClock = TestClock.createAndRegister();
  }

  @Test
  void taskProcessingInASpecificBucketIsNotAutomaticallyStarted() throws Exception {
    String st = "Hello World!";
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      assertEquals(st, task.getData());
      return new ProcessResult().setResultCode(ResultCode.DONE);
    });
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy().setProcessingBucket(BUCKET_ID));

    // Default bucket is auto started
    assertEquals(TasksProcessingState.STARTED, testTasksService.getTasksProcessingState(IBucketsManager.DEFAULT_ID));

    log.info("Submitting a task.");
    UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        testTasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString(st))
    ).getTaskId();

    assertEquals(TasksProcessingState.STOPPED, testTasksService.getTasksProcessingState(BUCKET_ID));
    assertEquals(TaskStatus.SUBMITTED.name(), taskDao.getTask(taskId, Task.class).getStatus());

    // lets assume task was stuck in processing
    assertTrue(taskDao.setStatus(taskId, TaskStatus.PROCESSING, 0));

    log.info("Moving clock forward 2 hours.");
    testClock.tick(Duration.ofHours(2));

    // task gets stuck-handled
    await().until(() ->
        TaskStatus.ERROR.name().equals(taskDao.getTask(taskId, Task.class).getStatus())
    );

    testTasksService.startTasksProcessing(BUCKET_ID);
    testTasksService.resumeTask(
        new ITasksService.ResumeTaskRequest()
            .setTaskId(taskId)
            .setVersion(taskDao.getTaskVersion(taskId)).setForce(true)
    );
    await().timeout(30, TimeUnit.SECONDS).until(() ->
       testTasksService.getFinishedTasks("test", null).size() == 1
    );

    assertEquals(TasksProcessingState.STARTED, testTasksService.getTasksProcessingState(BUCKET_ID));

    Future<Void> result = testTasksService.stopTasksProcessing(BUCKET_ID);
    result.get();

    assertEquals(TasksProcessingState.STOPPED, testTasksService.getTasksProcessingState(BUCKET_ID));
  }
}
