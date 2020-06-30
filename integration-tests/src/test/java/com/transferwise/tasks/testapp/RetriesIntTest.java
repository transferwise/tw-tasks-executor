package com.transferwise.tasks.testapp;

import static com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import com.transferwise.tasks.test.ITestTasksService;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
class RetriesIntTest extends BaseIntTest {

  @Autowired
  private TestTaskHandler testTaskHandlerAdapter;
  @Autowired
  private ITasksService tasksService;
  @Autowired
  private ITestTasksService testTasksService;
  @Autowired
  private ITransactionsHelper transactionsHelper;

  @Test
  void after5RetriesTasksGoToErrorState() {
    AtomicInteger processingCount = new AtomicInteger();
    int n = 5;

    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      log.info("Task retry nr: " + task.getProcessingTriesCount());
      processingCount.incrementAndGet();
      throw new RuntimeException("You can not pass, Frodo!");
    });

    testTaskHandlerAdapter.setRetryPolicy(
        new ExponentialTaskRetryPolicy()
            .setDelay(Duration.ofMillis(0))
            .setMaxCount(n)
            .setMultiplier(1)
    );

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setDataString("Hello World!")
            .setType("test"))
    );

    await().until(() -> processingCount.get() == n);
  }

  @ParameterizedTest(name = "Repeating cron task getProcessingTriesCount resetting {0} works, tries {1}")
  @MethodSource("casesForRepeatingCronTaskGetProcessingTriesWorks")
  void repeatingCronTaskGetProcessingTriesWorks(boolean resetTriesCountOnSuccess, int triesCount) {
    AtomicBoolean processed = new AtomicBoolean(false);
    AtomicLong triesCountInRetryPolicy = new AtomicLong();
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      processed.set(true);
      log.info("Task processed!");
      return new ProcessResult().setResultCode(COMMIT_AND_RETRY);
    });

    testTaskHandlerAdapter.setRetryPolicy(new ITaskRetryPolicy() {
      @Override
      public ZonedDateTime getRetryTime(ITask task, Throwable t) {
        triesCountInRetryPolicy.set(task.getProcessingTriesCount());
        return ZonedDateTime.ofInstant(ClockHolder.getClock().instant().plusSeconds(1000), ZoneOffset.UTC);
      }

      @Override
      public boolean resetTriesCountOnSuccess(IBaseTask task) {
        return resetTriesCountOnSuccess;
      }
    });

    UUID taskId = transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test")).getTaskId()
    );
    await().until(() -> {
      if (!processed.get()) {
        return false;
      }
      return testTasksService.getWaitingTasks("test", null).stream().anyMatch(t -> t.getId().equals(taskId));
    });

    assertEquals(triesCount, triesCountInRetryPolicy.get());
    assertEquals(
        triesCount,
        testTasksService.getWaitingTasks("test", null)
            .stream()
            .filter(t -> t.getId().equals(taskId)).findAny()
            .orElseThrow(IllegalStateException::new)
            .getProcessingTriesCount()
    );
  }

  @Test
  void after5RetriesAsynchronousTasksGoToErrorState() {
    AtomicInteger processingCount = new AtomicInteger();
    int n = 5;

    testTaskHandlerAdapter.setProcessor((IAsyncTaskProcessor) (task, successCallback, errorCallback) -> {
      log.info("Task retry nr: " + task.getProcessingTriesCount());
      processingCount.incrementAndGet();
      if (task.getProcessingTriesCount() > 2) {
        throw new RuntimeException("You can not pass, Frodo!");
      } else {
        new Thread(() -> errorCallback.accept(null)).start();
      }
    });

    testTaskHandlerAdapter.setRetryPolicy(
        new ExponentialTaskRetryPolicy()
            .setDelay(Duration.ofMillis(0))
            .setMaxCount(n)
            .setMultiplier(1)
    );

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(
            new ITasksService.AddTaskRequest()
                .setDataString("Hello World!")
                .setType("test")
        )
    );

    await().until(() -> processingCount.get() == n);
  }

  @Test
  void exponentialRetriesWorkEachRetryShouldTakeLonger() {
    TestClock clock = new TestClock();
    TwContextClockHolder.setClock(clock);
    
    AtomicInteger processingCount = new AtomicInteger();
    int n = 5;

    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      log.info("Task try #" + task.getProcessingTriesCount());
      processingCount.incrementAndGet();
      throw new RuntimeException("You can not pass, Legolas!");
    });

    testTaskHandlerAdapter.setRetryPolicy(
        new ExponentialTaskRetryPolicy()
            .setDelay(Duration.ofMillis(1000))
            .setMaxCount(n)
            .setMultiplier(2)
    );

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setDataString("Hello World!")
            .setType("test"))
    );

    await().until(() -> processingCount.get() == 1);

    for (int i = 2; i <= n; i++) {
      final int iCopy = i;
      long delay = 1000 * (long) Math.pow(2, (i - 2));

      await().until(() ->
          processingCount.get() == iCopy - 1 && !testTasksService.getWaitingTasks("test", null).isEmpty()
      );

      clock.tick(Duration.ofMillis(delay + 2));
      log.info("Time is now " + clock.instant());

      await().until(() -> processingCount.get() == iCopy);
    }

    assertEquals(n, processingCount.get());
  }

  private static Stream<Arguments> casesForRepeatingCronTaskGetProcessingTriesWorks() {
    return Stream.of(
        Arguments.of(true, 0),
        Arguments.of(false, 1)
    );
  }
}
