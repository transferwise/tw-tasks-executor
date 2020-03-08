package com.transferwise.tasks.health;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Meter.Id;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// TODO: Doesn't work if run solely, rework it
class ClusterWideTasksStateMonitorIntTest extends BaseIntTest {

  @Autowired
  private MeterRegistry meterRegistry;

  @Autowired
  private ClusterWideTasksStateMonitor clusterWideTasksStateMonitor;

  @Test
  void metricsAreCorrectlyRegistered() {
    Collection<Gauge> hlGauges = meterRegistry.get("twTasks.health.tasksHistoryLengthSeconds").gauges();

    assertEquals(2, hlGauges.size());
    assertTrue(hlGauges.stream().anyMatch(it -> "DONE".equals(it.getId().getTag("taskStatus"))));
    assertTrue(hlGauges.stream().anyMatch(it -> "ERROR".equals(it.getId().getTag("taskStatus"))));

    Gauge tasksInErrorCountGauge = meterRegistry.get("twTasks.health.tasksInErrorCount").gauge();
    Gauge stuckTasksGauge = meterRegistry.get("twTasks.health.stuckTasksCount").gauge();

    await().atMost(30, TimeUnit.SECONDS).until(() -> tasksInErrorCountGauge.value() == 0);
    await().until(() -> stuckTasksGauge.value() == 0);

    clusterWideTasksStateMonitor.leaderSelector.stop();

    await().until(() -> meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size() == 0);

    // metrics get unregistered
    assertEquals(0, meterRegistry.find("twTasks.health.tasksInErrorCount").gauges().size());
    assertEquals(0, meterRegistry.find("twTasks.health.stuckTasksCount").gauges().size());
    assertEquals(0, meterRegistry.find("twTasks.health.tasksHistoryLengthSeconds").gauges().size());

    // everything works when the node gets leader back
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

    assertEquals(1, meterRegistry.find("twTasks.health.tasksInErrorCount").gauge().value());

    // resetting the task
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.resetAndDeleteTasksWithTypes("test");
      return null;
    });

    clusterWideTasksStateMonitor.check();

    assertEquals(0, meterRegistry.find("twTasks.health.tasksInErrorCount").gauge().value());
    assertTrue(
        meterRegistry.getMeters()
            .stream()
            .map(Meter::getId)
            .map(Id::getName)
            .noneMatch("twTasks.health.tasksInErrorCountPerType"::equals)
    );
  }
}
