package com.transferwise.tasks.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
class ClusterWideTasksStateMonitorIntTest extends BaseIntTest {

  @Autowired
  private MeterRegistry meterRegistry;
  @Autowired
  private ClusterWideTasksStateMonitor clusterWideTasksStateMonitor;
  @Autowired
  private TasksProperties tasksProperties;

  private Duration originalStartDelay;

  @BeforeEach
  void setup() {
    originalStartDelay = tasksProperties.getClusterWideTasksStateMonitor().getStartDelay();
  }

  @AfterEach
  void cleanup() {
    tasksProperties.getClusterWideTasksStateMonitor().setStartDelay(originalStartDelay);
  }

  @Test
  void metricsAreCorrectlyRegistered() {
    clusterWideTasksStateMonitor.resetState(false);
    clusterWideTasksStateMonitor.resetState(true);
    clusterWideTasksStateMonitor.check();

    Collection<Gauge> hlGauges = meterRegistry.get("twTasks.health.tasksHistoryLengthSeconds").gauges();

    assertEquals(2, hlGauges.size());
    assertTrue(hlGauges.stream().anyMatch(it -> "DONE".equals(it.getId().getTag("taskStatus"))));
    assertTrue(hlGauges.stream().anyMatch(it -> "ERROR".equals(it.getId().getTag("taskStatus"))));

    Gauge tasksInErrorCountGauge = meterRegistry.get("twTasks.health.tasksInErrorCount").gauge();
    Gauge stuckTasksGauge = meterRegistry.get("twTasks.health.stuckTasksCount").gauge();

    await().atMost(30, TimeUnit.SECONDS).until(() -> tasksInErrorCountGauge.value() == 0);
    await().until(() -> stuckTasksGauge.value() == 0);

    assertThat(meterRegistry.get("twTasks.state.approximateTasks").gauge().value()).isGreaterThan(-1);
    assertThat(meterRegistry.get("twTasks.state.approximateUniqueKeys").gauge().value()).isGreaterThan(-1);

    clusterWideTasksStateMonitor.leaderSelector.stop();

    await().until(() -> meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 0);

    // metrics get unregistered
    assertEquals(0, meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size());
    assertEquals(0, meterRegistry.find("twTasks.health.stuckTasksCount").gauges().size());
    assertEquals(0, meterRegistry.find("twTasks.health.tasksHistoryLengthSeconds").gauges().size());
    assertThat(meterRegistry.find("twTasks.state.approximateTasks").gauges().size()).isEqualTo(0);
    assertThat(meterRegistry.find("twTasks.state.approximateUniqueKeys").gauges().size()).isEqualTo(0);

    // everything works when the node gets leader back
    tasksProperties.getClusterWideTasksStateMonitor().setStartDelay(Duration.ZERO);
    clusterWideTasksStateMonitor.leaderSelector.start();

    await().until(() -> meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 1);

    // creating a task that goes to error
    testTaskHandlerAdapter.setProcessor((ISyncTaskProcessor) task -> {
      throw new IllegalStateException("We want an error!");
    });
    transactionsHelper.withTransaction().asNew().call(() ->
        testTasksService.addTask(new ITasksService.AddTaskRequest()
            .setDataString("Hello World!")
            .setType("test"))
    );

    // wait until it's processed and gets to error state
    await().until(() -> testTasksService.getTasks("test", null, TaskStatus.ERROR).size() == 1);

    clusterWideTasksStateMonitor.check();

    assertEquals(1, meterRegistry.get("twTasks.health.tasksInErrorCount").gauge().value());

    // resetting the task
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.resetAndDeleteTasksWithTypes("test");
      return null;
    });

    clusterWideTasksStateMonitor.check();

    assertEquals(0, meterRegistry.get("twTasks.health.tasksInErrorCount").gauge().value());
    assertTrue(
        meterRegistry.getMeters()
            .stream()
            .map(Meter::getId)
            .map(Id::getName)
            .noneMatch("twTasks.health.tasksInErrorCountPerType"::equals)
    );
  }

  @Test
  @SneakyThrows
  void metricsAreProbablyThreadSafe() {
    final int T = 20;
    final int N = 10000;
    ExecutorService executor = Executors.newFixedThreadPool(T);

    AtomicInteger errorsCount = new AtomicInteger();
    AtomicInteger executionsCount = new AtomicInteger();

    Consumer<Runnable> wrapper = (runnable) -> {
      executionsCount.incrementAndGet();
      try {
        runnable.run();
      } catch (Throwable t) {
        log.error(t.getMessage(), t);
        errorsCount.incrementAndGet();
      }
    };

    for (int i = 0; i < N; i++) {
      if (i % 4 == 0) {
        executor.submit(() -> wrapper.accept(() -> {
          clusterWideTasksStateMonitor.resetState(true);
          clusterWideTasksStateMonitor.resetState(false);
        }));
      } else {
        executor.submit(() -> wrapper.accept(() -> clusterWideTasksStateMonitor.check()));
      }
    }

    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.SECONDS);

    assertEquals(0, errorsCount.get());
  }

}
