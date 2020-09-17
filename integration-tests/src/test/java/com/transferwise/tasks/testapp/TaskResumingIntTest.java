package com.transferwise.tasks.testapp;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.test.ITestTasksService;
import io.micrometer.core.instrument.Counter;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TaskResumingIntTest extends BaseIntTest {

  @Autowired
  private ITasksService tasksService;
  @Autowired
  private ITestTasksService testTasksService;

  @BeforeEach
  void setup() {
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });
  }

  @Test
  void taskCanBeSuccessfullyResumed() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setDataString("Hello World!")
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> testTasksService.getWaitingTasks("test", null).size() > 0);
    assertTrue(transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(0))
    ));
    await().until(() -> resultRegisteringSyncTaskProcessor.getTaskResults().get(taskId) != null);
  }

  @Test
  void taskWillNotBeResumedIfVersionHasAlreadyChanged() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    final long initialFailedStatusChangeCount = getFailedStatusChangeCount();
    final UUID taskId = UuidUtils.generatePrefixCombUuid();

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setTaskId(taskId)
            .setDataString("Hello World!")
            .setType("test").setRunAfterTime(ZonedDateTime.now().plusHours(1)))
    );

    await().until(() -> testTasksService.getWaitingTasks("test", null).size() > 0);
    assertFalse(
        transactionsHelper.withTransaction().asNew().call(() ->
            tasksService.resumeTask(new ITasksService.ResumeTaskRequest().setTaskId(taskId).setVersion(-1))
        )
    );
    assertEquals(initialFailedStatusChangeCount + 1, getFailedStatusChangeCount());
  }

  private long getFailedStatusChangeCount() {
    Counter counter = meterRegistry.find("twTasks.tasks.failedStatusChangeCount").tags(
        "taskType", "test",
        "fromStatus", "WAITING",
        "toStatus", "SUBMITTED"
    ).counter();

    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }
}
