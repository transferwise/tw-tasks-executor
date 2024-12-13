package com.transferwise.tasks.ext.jobs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.transferwise.common.baseutils.clock.TestClock;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import com.transferwise.tasks.impl.jobs.CronJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.test.ITestJobsService;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

class JobsIntTest extends BaseIntTest {

  @Autowired
  private ConfigurableApplicationContext applicationContext;

  @Autowired
  private ITaskDao taskDao;

  @Autowired
  private ITestJobsService testJobsService;

  @Autowired
  private CronJobA cronJobA;

  @BeforeEach
  void resetJobs() {
    testJobsService.reset();
  }

  @Test
  void registerCronJobBean() {
    int executionsCount = cronJobA.executionsCount;
    await().until(() -> cronJobA.executionsCount == executionsCount + 2);
  }

  @Test
  void jobExecutes5TimesAndThenEnds() {
    JobA jobA = registerJobBean(new JobA());

    await().until(() -> jobA.executionsCount == 5);
    await().until(() -> testTasksService.getFinishedTasks("TaskJob|JobA", null).size() == 1);

    // processing tries are reset after every success call
    Task task = taskDao.getTask(jobA.getTaskId(), Task.class);
    assertThat(task.getProcessingTriesCount()).isEqualTo(1);
    assertThat(task.getStatus()).isEqualTo(TaskStatus.DONE.name());
  }

  @Test
  void intermediateRetriesWorkCorrectly() {
    IntermediateRetriesTestSetup setup = new IntermediateRetriesTestSetup();
    registerJobBean(setup.job);

    assertThat(setup.job.executionsCount).isZero();

    // jobs runs every 60seconds, triggering the first run
    setup.tickGivenSecondsPlusOneMillisecond(60);
    setup.waitForExecutionCount(1);

    // job will retry 3 times, overall 4 times
    for (int i = 0; i < 3; i++) {
      setup.tickGivenSecondsPlusOneMillisecond(1);
      setup.waitForExecutionCount(2 + i);
    }

    // last retry succeeded, expected job to get back to normal execution, overall should be 5 retries
    setup.tickGivenSecondsPlusOneMillisecond(60);
    setup.waitForExecutionCount(5);

    // another 3 failures, results in 8 tries overall
    for (int i = 0; i < 3; i++) {
      setup.tickGivenSecondsPlusOneMillisecond(1);
      setup.waitForExecutionCount(6 + i);
    }

    // another normal retry, results in 9 tries
    setup.tickGivenSecondsPlusOneMillisecond(60);
    setup.waitForExecutionCount(9);

    // and the last one
    setup.tickGivenSecondsPlusOneMillisecond(60);

    // the task is finished
    await().until(() -> testTasksService.getFinishedTasks("TaskJob|MyJobB", null).size() == 1);

    assertThat(setup.job.executionsCount).isEqualTo(10);
  }

  @Test
  void jobCanBeExecutedFromTest() {
    JobC job = registerJobBean(new JobC());

    assertThat(job.executionsCount).isZero();

    ITestJobsService.ExecuteAsyncHandle handle = testJobsService.executeAsync(job);

    await().until(() -> testJobsService.hasFinished(handle));
    assertThat(job.executionsCount).isEqualTo(1);
  }

  @Test
  void onlySelectedJobsWillExecute() {
    JobC job = new JobC();
    testJobsService.resetAndInitialize(List.of(job));

    // manually triggered job is initialized and executed
    assertThat(job.executionsCount).isZero();

    ITestJobsService.ExecuteAsyncHandle handle = testJobsService.executeAsync(job);

    await().until(() -> testJobsService.hasFinished(handle));
    assertThat(job.executionsCount).isEqualTo(1);

    // other did not initialize
    assertThat(testTasksService.getTasks("TaskJob|JobA", null)).isEmpty();
  }

  private <T extends IJob> T registerJobBean(T job) {
    applicationContext.getBeanFactory().registerSingleton(job.getClass().getSimpleName(), job);
    testJobsService.reset();
    return job;
  }

  static class JobA implements IJob {

    int executionsCount;

    @Override
    public ZonedDateTime getNextRunTime() {
      return ZonedDateTime.now(TwContextClockHolder.getClock());
    }

    @Override
    public ProcessResult process(ITask task) {
      executionsCount += 1;
      if (executionsCount == 5) {
        return new ProcessResult().setResultCode(ProcessResult.ResultCode.END);
      }
      return ThreadLocalRandom.current().nextBoolean() ? new ProcessResult().setResultCode(ProcessResult.ResultCode.SUCCESS) : null;
    }
  }

  @Component
  static class CronJobA {

    int executionsCount;

    @CronJob("* * * * * *")
    void annotatedMethod() {
      executionsCount++;
    }
  }

  @Slf4j
  static class JobB implements IJob {

    private final String script = "RFFFRFFFRR";
    private int executionsCount;
    private char retryCode;

    @Override
    public String getUniqueName() {
      return "MyJobB";
    }

    @Override
    public ITaskRetryPolicy getFailureRetryPolicy() {
      return (task, t) -> {
        if (script.charAt(executionsCount) != 'F') {
          throw new RuntimeException("Massive fail! Failure retry policy should not have been used.");
        }
        retryCode = 'F';
        log.info("Next runtime will be after 1s");
        return ZonedDateTime.now(TwContextClockHolder.getClock()).plusSeconds(1);
      };
    }

    @Override
    public ZonedDateTime getNextRunTime() {
      retryCode = 'R';
      log.info("Next runtime will be after 60s");
      return ZonedDateTime.now(TwContextClockHolder.getClock()).plusSeconds(60);
    }

    @Override
    public ProcessResult process(ITask task) {
      log.info("Proccesing task.");
      if (retryCode != script.charAt(executionsCount)) {
        log.error("Massive fail!");
      }
      executionsCount++;
      if (executionsCount == script.length()) {
        return new ProcessResult().setResultCode(ProcessResult.ResultCode.END);
      }
      if (script.charAt(executionsCount) == 'F') {
        throw new RuntimeException("Triggering failure.");
      }
      return null;
    }
  }

  static class JobC implements IJob {

    private int executionsCount;

    @Override
    public ZonedDateTime getNextRunTime() {
      return ZonedDateTime.now(TwContextClockHolder.getClock()).plusYears(100);
    }

    @Override
    public ProcessResult process(ITask task) {
      executionsCount++;
      return null;
    }
  }

  private class IntermediateRetriesTestSetup {

    final TestClock clock = new TestClock();
    final JobB job = new JobB();

    public IntermediateRetriesTestSetup() {
      TwContextClockHolder.setClock(clock);
    }

    void waitForExecutionCount(int expectedCount) {
      await().until(
          () -> job.executionsCount == expectedCount && testTasksService.getWaitingTasks("TaskJob|MyJobB", null).size() > 0
      );
    }

    void tickGivenSecondsPlusOneMillisecond(int seconds) {
      clock.tick(Duration.ofSeconds(seconds).plusMillis(1));
    }
  }
}