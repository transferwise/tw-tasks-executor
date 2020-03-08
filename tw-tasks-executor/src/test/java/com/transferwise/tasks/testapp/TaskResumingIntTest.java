package com.transferwise.tasks.testapp;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.test.ITestTasksService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
public class TaskResumingIntTest extends BaseIntTest {

  @Autowired
  private ITasksService tasksService;
  @Autowired
  private ITestTasksService testTasksService;
  @Autowired
  protected TestTaskHandler testTaskHandlerAdapter;
  @Autowired
  protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor;
  @Autowired
  protected ITransactionsHelper transactionsHelper;
  @Autowired
  protected MeterRegistry meterRegistry;

  @BeforeEach
  void setup() {
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      return null;
    });
  }

  @Test
  void aTaskCanBeSuccessfullyResumed() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    UUID taskId = UUID.randomUUID();

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
    long initialFailedStatusChangeCount = getFailedStatusChangeCount();
    UUID taskId = UUID.randomUUID();

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
