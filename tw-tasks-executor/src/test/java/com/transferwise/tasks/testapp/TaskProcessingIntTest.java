package com.transferwise.tasks.testapp;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDao.DaoTask1;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.Advised;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TaskProcessingIntTest extends BaseIntTest {

  @Autowired
  protected ITasksService tasksService;
  @Autowired
  protected TestTaskHandler testTaskHandlerAdapter;
  @Autowired
  protected ITaskDao taskDao;
  @Autowired
  protected IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor;
  @Autowired
  protected ITestTasksService testTasksService;
  @Autowired
  protected ITransactionsHelper transactionsHelper;
  @Autowired
  protected ITasksExecutionTriggerer tasksExecutionTriggerer;
  @Autowired
  protected MeterRegistry meterRegistry;

  private KafkaTasksExecutionTriggerer kafkaTasksExecutionTriggerer;

  @BeforeEach
  void setup() throws Exception {
    kafkaTasksExecutionTriggerer = (KafkaTasksExecutionTriggerer) ((Advised) tasksExecutionTriggerer).getTargetSource().getTarget();
  }

  @Test
  void allUniqueTasksWillGetProcessed() throws Exception {
    final int initialProcessingsCount = counterSum("twTasks.tasks.processingsCount");
    final int initialProcessedCount = counterSum("twTasks.tasks.processedCount");
    final int initialDuplicatesCount = counterSum("twTasks.tasks.duplicatesCount");
    final long initialSummaryCount = timerSum("twTasks.tasks.processingTime");

    final int duplicatesMultiplier = 2;
    final int uniqueTasksCount = 500;
    final int submittingThreadsCount = 10;
    final int taskProcessingConcurrency = 100;

    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    testTaskHandlerAdapter.setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(taskProcessingConcurrency));

    // when
    ExecutorService executorService = Executors.newFixedThreadPool(submittingThreadsCount);

    for (int j = 0; j < duplicatesMultiplier; j++) {
      for (int i = 0; i < uniqueTasksCount; i++) {
        final int key = i;
        executorService.submit(() -> {
          try {
            tasksService.addTask(new ITasksService.AddTaskRequest()
                .setDataString("Hello World! " + key)
                .setType("test")
                .setKey(String.valueOf(key)));
          } catch (Throwable t) {
            log.error(t.getMessage(), t);
          }
        });
      }
    }

    log.info("Waiting for all tasks to be registered.");
    executorService.shutdown();
    executorService.awaitTermination(15, TimeUnit.SECONDS);

    log.info("All tasks have been registered.");
    long start = System.currentTimeMillis();

    await().until(() -> resultRegisteringSyncTaskProcessor.getTaskResults().size() == uniqueTasksCount);

    long end = System.currentTimeMillis();
    log.info("Tasks execution took {} ms", end - start);

    KafkaTasksExecutionTriggerer.ConsumerBucket consumerBucket = kafkaTasksExecutionTriggerer.getConsumerBucket("default");
    assertEquals(0, consumerBucket.getOffsetsCompletedCount());
    assertEquals(0, consumerBucket.getOffsetsCount());
    assertEquals(0, consumerBucket.getUnprocessedFetchedRecordsCount());

    await().until(() -> consumerBucket.getOffsetsToBeCommitedCount() == 0);
    assertEquals(uniqueTasksCount, resultRegisteringSyncTaskProcessor.getTaskResults().size());

    // instrumentation assertion
    assertEquals(uniqueTasksCount + initialProcessingsCount, counterSum("twTasks.tasks.processingsCount"));
    assertEquals(uniqueTasksCount + initialProcessedCount, counterSum("twTasks.tasks.processedCount"));
    assertEquals(uniqueTasksCount + initialDuplicatesCount, counterSum("twTasks.tasks.duplicatesCount"));
    assertEquals(uniqueTasksCount + initialSummaryCount, timerSum("twTasks.tasks.processingTime"));

    assertEquals(ITasksService.TasksProcessingState.STARTED, tasksService.getTasksProcessingState(null));
  }

  @Test
  void taskRunningForTooLongWillBeHandled() throws Exception {
    ResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor = new ResultRegisteringSyncTaskProcessor() {
      @Override
      public ISyncTaskProcessor.ProcessResult process(ITask task) {
        log.info("Starting a long running task {}", task.getVersionId());
        ExceptionUtils.doUnchecked(() -> Thread.sleep(100_000));
        log.info("Finished. Now marking as processed {}", task.getVersionId());
        return super.process(task);
      }
    };

    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    testTaskHandlerAdapter.setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(100));
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy()
        .setStuckTaskResolutionStrategy(ITaskProcessingPolicy.StuckTaskResolutionStrategy.MARK_AS_ERROR)
        .setMaxProcessingDuration(Duration.ofMillis(100))
    );

    AtomicReference<ITasksService.AddTaskResponse> taskRef = new AtomicReference<>();
    Thread thread = new Thread(() ->  // Just to trigger the task
        taskRef.set(
            tasksService.addTask(new ITasksService.AddTaskRequest()
                .setDataString("Hello World! 1")
                .setType("test")
                .setKey("1")
            )
        )
    );
    thread.start();
    thread.join();

    long start = System.currentTimeMillis();

    await().atMost(30, TimeUnit.SECONDS).until(() -> {
      int proceessedTasksCount = resultRegisteringSyncTaskProcessor.getTaskResults().size();
      // Will explode if we restarted stuck task above.
      if (proceessedTasksCount > 1) {
        throw new RuntimeException("We have more running tasks than we expect");
      }

      List<DaoTask1> error = transactionsHelper.withTransaction().asNew().call(() ->
          taskDao.getTasksInErrorStatus(10)
      );
      boolean taskWasMarkedAsError = error.size() != 0 && error.get(0).getId().equals(taskRef.get().getTaskId());
      if (taskWasMarkedAsError) {
        log.info("StuckTaskResolution worked correctly, task {} was marked as ERROR", taskRef.get().getTaskId());
      }
      if (proceessedTasksCount == 1) {
        throw new RuntimeException(
            "Let's fail, as it means that task completed earlier, than was marked"
                + " as error, which probably means that stuck task processor didn't work"
        );
      }
      return taskWasMarkedAsError;
    });

    long end = System.currentTimeMillis();

    log.info("Tasks execution took {} ms.", end - start);

    assertEquals(1, getCountOfMarkedAsErrorTasks());
  }

  @Test
  void taskWithHugeMessageCanBeHandled() {
    StringBuilder sb = new StringBuilder();
    // 11MB
    for (int i = 0; i < 1000 * 1000; i++) {
      sb.append("Hello World!");
    }
    String st = sb.toString();
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      assertEquals(st, task.getData());
      return new ProcessResult().setResultCode(ResultCode.DONE);
    });

    log.info("Submitting huge message task.");
    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString(st))
    );
    await().until(() -> transactionsHelper.withTransaction().asNew().call(() -> {
      try {
        return testTasksService.getFinishedTasks("test", null).size() == 1;
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
      return false;
    }));
  }

  @Test
  void taskWithUnknownBucketIsMovedToErrorState() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy().setProcessingBucket("unknown-bucket"));

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setDataString("Hello World!"))
    );

    await().until(() -> {
      try {
        return transactionsHelper.withTransaction().asNew().call(() ->
            testTasksService.getTasks("test", null, TaskStatus.ERROR).size() == 1
        );
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
      }
      return false;
    });
  }

  private int counterSum(String name) {
    return meterRegistry.find(name)
        .counters()
        .stream().mapToInt(c -> (int) c.count())
        .sum();
  }

  private long timerSum(String name) {
    return meterRegistry.find(name)
        .timers()
        .stream()
        .mapToLong(Timer::count)
        .sum();
  }

  private long getCountOfMarkedAsErrorTasks() {
    Counter counter = meterRegistry.find("twTasks.tasks.markedAsErrorCount").counter();
    if (counter == null) {
      return 0;
    } else {
      return (long) counter.count();
    }
  }
}
