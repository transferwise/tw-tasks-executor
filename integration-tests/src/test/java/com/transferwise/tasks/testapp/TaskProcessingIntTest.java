package com.transferwise.tasks.testapp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.context.Criticality;
import com.transferwise.common.context.TwContext;
import com.transferwise.common.context.UnitOfWork;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.management.dao.IManagementTaskDao;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask1;
import com.transferwise.tasks.processing.GlobalProcessingState;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.triggering.KafkaTasksExecutionTriggerer;
import io.micrometer.core.instrument.Timer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TaskProcessingIntTest extends BaseIntTest {

  @Autowired
  protected ITasksService tasksService;
  @Autowired
  protected ITaskDao taskDao;
  @Autowired
  protected IManagementTaskDao managementTaskDao;
  @Autowired
  protected ITasksExecutionTriggerer tasksExecutionTriggerer;
  @Autowired
  protected ITaskDataSerializer taskDataSerializer;
  @Autowired
  protected GlobalProcessingState globalProcessingState;

  private KafkaTasksExecutionTriggerer kafkaTasksExecutionTriggerer;

  @BeforeEach
  void setup() {
    kafkaTasksExecutionTriggerer = (KafkaTasksExecutionTriggerer) tasksExecutionTriggerer;
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
            tasksService.addTask(new AddTaskRequest()
                .setData(taskDataSerializer.serialize("Hello World! " + key))
                .setType("test")
                .setUniqueKey(String.valueOf(key)));
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
    final long initialMarkedAsErrors = getCountOfMarkedAsErrorTasks();

    IResultRegisteringSyncTaskProcessor resultRegisteringSyncTaskProcessor = new ResultRegisteringSyncTaskProcessor() {
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
                .setData(taskDataSerializer.serialize("Hello World! 1"))
                .setType("test")
                .setUniqueKey("1")
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
          managementTaskDao.getTasksInErrorStatus(10)
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

    await().until(() -> 1 + initialMarkedAsErrors == getCountOfMarkedAsErrorTasks());
  }

  @Test
  void taskWithHugeMessageCanBeHandled() {
    StringBuilder sb = new StringBuilder();
    // 11MB
    for (int i = 0; i < 1000 * 1000; i++) {
      sb.append("Hello World!");
    }
    String st = sb.toString();
    byte[] stBytes = st.getBytes(StandardCharsets.UTF_8);
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      assertThat(task.getData()).isEqualTo(stBytes);
      return new ProcessResult().setResultCode(ResultCode.DONE);
    });

    log.info("Submitting huge message task.");
    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setData(taskDataSerializer.serialize(st)))
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

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 5, 9, 10})
  void taskWithSpecificPriorityCanBeHandled(int priority) {
    String st = "Hello World!";
    byte[] stBytes = st.getBytes(StandardCharsets.UTF_8);
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      assertThat(task.getData()).isEqualTo(stBytes);
      return new ProcessResult().setResultCode(ResultCode.DONE);
    });

    log.info("Submitting huge message task.");
    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setPriority(priority).setData(taskDataSerializer.serialize(st)))
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
  @SneakyThrows
  void highestPriorityTasksWillBeProcessedFirst() {
    List<Integer> processedPriorities = new CopyOnWriteArrayList<>();

    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      if (processedPriorities.size() == 0) {
        // Wait until the in memory table has received enough entries from Kafka.
        await().until(() -> globalProcessingState.getBuckets().get("default").getSize().get() > 2);
      }

      processedPriorities.add(task.getPriority());
      return new ProcessResult().setResultCode(ResultCode.DONE);
    });

    testTasksService.stopProcessing();

    transactionsHelper.withTransaction().asNew().call(() -> {
      tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setPriority(7));
      tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setPriority(1));
      tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setPriority(4));
      tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setPriority(2));
      return null;
    });

    testTasksService.resumeProcessing();

    await().until(() -> processedPriorities.size() == 4);

    // As task finding loop and a thread filling tasks memory table are running in parallel, it is likely, that
    // first task is not "1". Rest however should be processed in expected order.
    assertThat(processedPriorities.subList(1, 4)).isSorted();
  }

  @Test
  void taskWithUnknownBucketIsMovedToErrorState() {
    testTaskHandlerAdapter.setProcessor(resultRegisteringSyncTaskProcessor);
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy().setProcessingBucket("unknown-bucket"));

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test").setData(taskDataSerializer.serialize("Hello World!")))
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

  @Test
  void taskProcessorHasCorrectContextSet() {
    final MutableObject<String> entryPointNameRef = new MutableObject<>();
    final MutableObject<String> entryPointGroupRef = new MutableObject<>();
    final MutableObject<Map<String, String>> mdcMapRef = new MutableObject<>();
    final MutableObject<Criticality> criticalityRef = new MutableObject<>();
    final MutableObject<Instant> deadlineRef = new MutableObject<>();
    final MutableObject<String> ownerRef = new MutableObject<>();

    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      mdcMapRef.setValue(MDC.getCopyOfContextMap());
      TwContext twContext = TwContext.current();
      entryPointNameRef.setValue(twContext.getName());
      entryPointGroupRef.setValue(twContext.getGroup());
      ownerRef.setValue(twContext.getOwner());
      UnitOfWork unitOfWork = twContext.get(UnitOfWork.TW_CONTEXT_KEY);
      criticalityRef.setValue(unitOfWork.getCriticality());
      deadlineRef.setValue(unitOfWork.getDeadline());
      log.info("Test task for 'taskProcessorHasCorrectContextSet' finished.");
      return null;
    });
    testTaskHandlerAdapter.setProcessingPolicy(new SimpleTaskProcessingPolicy().setCriticality(Criticality.CRITICAL_PLUS)
        .setOwner("TransferWise"));

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService
            .addTask(new ITasksService.AddTaskRequest().setType("test").setSubType("mdc").setData(taskDataSerializer.serialize("Hello World!")))
    );

    Task task = await().until(() -> transactionsHelper.withTransaction().asNew().call(
        () -> {
          try {
            return testTasksService.getFinishedTasks("test", null);
          } catch (Throwable t) {
            log.error(t.getMessage(), t);
          }
          return null;
        }),
        res -> {
          if (res == null) {
            return false;
          }
          return res.size() == 1;
        }).get(0);

    Map<String, String> mdcMap = mdcMapRef.getValue();
    assertThat(mdcMap.get("twTaskId")).isEqualTo(task.getId().toString());
    assertThat(mdcMap.get("twTaskVersion")).isEqualTo("1");
    assertThat(mdcMap.get("twTaskType")).isEqualTo("test");
    assertThat(mdcMap.get("twTaskSubType")).isEqualTo("mdc");
    assertThat(entryPointNameRef.getValue()).isEqualTo("Processing_test");
    assertThat(entryPointGroupRef.getValue()).isEqualTo("TwTasks");
    assertThat(ownerRef.getValue()).isEqualTo("TransferWise");
    assertThat(criticalityRef.getValue()).isEqualTo(Criticality.CRITICAL_PLUS);
    assertThat(deadlineRef.getValue()).isAfter(Instant.now());
  }

  @Test
  void taskWithRateLimitingConcurrencyPolicyWillNotHang() {
    AtomicInteger counter = new AtomicInteger();
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> new ProcessResult().setResultCode(ResultCode.DONE));

    testTaskHandlerAdapter.setConcurrencyPolicy(new ITaskConcurrencyPolicy() {
      @Override
      public @NonNull BookSpaceResponse bookSpace(IBaseTask task) {
        if (counter.incrementAndGet() < 5) {
          return new BookSpaceResponse(false).setTryAgainTime(Instant.now().plusMillis(10));
        }
        return new BookSpaceResponse(true);
      }

      @Override
      public void freeSpace(IBaseTask task) {

      }
    });

    transactionsHelper.withTransaction().asNew().call(() ->
        tasksService.addTask(new ITasksService.AddTaskRequest().setType("test"))
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

  private int counterSum(String name) {
    return meterRegistry.find(name)
        .counters()
        .stream().mapToInt(c -> (int) c.count())
        .sum();
  }

  @SuppressWarnings("SameParameterValue")
  private long timerSum(String name) {
    return meterRegistry.find(name)
        .timers()
        .stream()
        .mapToLong(Timer::count)
        .sum();
  }

  private long getCountOfMarkedAsErrorTasks() {
    return meterRegistry.find("twTasks.tasks.markedAsErrorCount")
        .counters()
        .stream()
        .mapToInt(c -> (int) c.count())
        .sum();
  }
}
