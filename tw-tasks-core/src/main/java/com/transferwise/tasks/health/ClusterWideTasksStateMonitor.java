package com.transferwise.tasks.health;

import static com.transferwise.tasks.helpers.IMeterHelper.METRIC_PREFIX;

import com.google.common.collect.ImmutableMap;
import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.LeaderSelector;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.IMeterHelper;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class ClusterWideTasksStateMonitor implements ITasksStateMonitor, GracefulShutdownStrategy {

  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private IMeterHelper meterHelper;
  @Autowired
  private CuratorFramework curatorFramework;

  LeaderSelector leaderSelector;

  private List<Pair<String, Integer>> erroneousTasksCountPerType;
  private Map<String, AtomicInteger> erroneousTasksCounts;
  private AtomicInteger erroneousTasksCount;

  private AtomicInteger stuckTasksCount;

  private Map<TaskStatus, AtomicLong> tasksHistoryLengthSeconds;

  private List<Object> registeredMetricHandles;
  private Map<String, Object> taskInErrorStateHandles;

  @PostConstruct
  public void init() {
    String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_state_monitor";

    ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-tsm", executorServicesProvider.getGlobalExecutorService());
    leaderSelector = new LeaderSelector(curatorFramework, nodePath, executorService,
        control -> {
          ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
          MutableObject<ScheduledTaskExecutor.TaskHandle> taskHandleHolder = new MutableObject<>();

          control.workAsyncUntilShouldStop(
              () -> {
                resetState();
                taskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(this::check, Duration.ofSeconds(0),
                    Duration.ofSeconds(30)));
                log.info("Started to monitor tasks state.");
              },
              () -> {
                log.info("Stopping monitoring of tasks state.");
                if (taskHandleHolder.getValue() != null) {
                  taskHandleHolder.getValue().stop();
                  taskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
                }
                resetState();
                log.info("Monitoring of tasks state stopped.");
              });
        });
  }

  protected void resetState() {
    if (registeredMetricHandles != null) {
      for (Object metricHandle : registeredMetricHandles) {
        meterHelper.unregisterMetric(metricHandle);
      }
    }

    stuckTasksCount = null;

    erroneousTasksCount = null;
    erroneousTasksCounts = new ConcurrentHashMap<>();
    erroneousTasksCountPerType = new ArrayList<>();
    registeredMetricHandles = new ArrayList<>();
    taskInErrorStateHandles = new HashMap<>();
    tasksHistoryLengthSeconds = new HashMap<>();
  }

  protected void check() {
    checkErroneousTasks();
    checkStuckTasks();
    measureTasksHistoryLength();
  }

  protected void measureTasksHistoryLength() {
    measureTaskHistoryLength(TaskStatus.DONE);
    measureTaskHistoryLength(TaskStatus.ERROR);
  }

  protected void measureTaskHistoryLength(TaskStatus status) {
    ZonedDateTime now = ZonedDateTime.now(ClockHolder.getClock());
    ZonedDateTime earliestTaskNextEventTime = taskDao.getEarliestTaskNextEventTime(status);
    long historyLengthSeconds = earliestTaskNextEventTime == null ? 0L : Duration.between(earliestTaskNextEventTime, now).getSeconds();

    AtomicLong counter = tasksHistoryLengthSeconds.get(status);
    boolean metricRegistered = counter != null;
    if (!metricRegistered) {
      tasksHistoryLengthSeconds.put(status, counter = new AtomicLong());
    }

    counter.set(historyLengthSeconds);

    if (!metricRegistered) {
      registeredMetricHandles
          .add(meterHelper.registerGauge(METRIC_PREFIX + "health.tasksHistoryLengthSeconds", ImmutableMap.of("taskStatus", status.name()),
              counter::get));
    }
  }

  protected void checkErroneousTasks() {
    boolean totalGaugeNotRegistered = erroneousTasksCount == null;
    if (totalGaugeNotRegistered) {
      erroneousTasksCount = new AtomicInteger();
    }
    erroneousTasksCount.set(taskDao.getTasksCountInStatus(tasksProperties.getMaxDatabaseFetchSize(), TaskStatus.ERROR));
    if (erroneousTasksCount.get() == 0) {
      erroneousTasksCountPerType = Collections.emptyList();
    } else {
      erroneousTasksCountPerType = taskDao.getTasksCountInErrorGrouped(tasksProperties.getMaxDatabaseFetchSize());
    }

    if (totalGaugeNotRegistered) {
      AtomicInteger erroneousTasksCountCopy = erroneousTasksCount;
      registeredMetricHandles.add(meterHelper.registerGauge(METRIC_PREFIX + "health.tasksInErrorCount", erroneousTasksCountCopy::get));
    }

    Set<String> erroneousTaskTypes = new HashSet<>();
    for (Pair<String, Integer> typeInError : erroneousTasksCountPerType) {
      erroneousTaskTypes.add(typeInError.getKey());
      AtomicInteger typeCounter = erroneousTasksCounts.computeIfAbsent(typeInError.getKey(), (k) -> {
        AtomicInteger cnt = new AtomicInteger();
        Object handle = meterHelper
            .registerGauge(METRIC_PREFIX + "health.tasksInErrorCountPerType", ImmutableMap.of("taskType", typeInError.getKey()), cnt::get);
        registeredMetricHandles.add(handle);
        taskInErrorStateHandles.put(typeInError.getKey(), handle);
        return cnt;
      });
      typeCounter.set(typeInError.getValue());
    }

    // make sure that we reset values for the tasks that are not in error state anymore
    for (Iterator<String> it = erroneousTasksCounts.keySet().iterator(); it.hasNext(); ) {
      String taskType = it.next();
      if (!erroneousTaskTypes.contains(taskType)) {
        Object handle = taskInErrorStateHandles.remove(taskType);
        registeredMetricHandles.remove(handle);
        meterHelper.unregisterMetric(handle);
        it.remove();
      }
    }
  }

  protected void checkStuckTasks() {
    boolean stuckTasksMeterNotRegistered = stuckTasksCount == null;
    if (stuckTasksMeterNotRegistered) {
      stuckTasksCount = new AtomicInteger();
    }
    stuckTasksCount
        .set(taskDao.getStuckTasksCount(ZonedDateTime.now(ClockHolder.getClock()).minus(tasksProperties.getStuckTaskAge()),
            tasksProperties.getMaxDatabaseFetchSize()));
    if (stuckTasksMeterNotRegistered) {
      AtomicInteger stuckTasksCountCopy = stuckTasksCount;
      registeredMetricHandles.add(meterHelper.registerGauge(METRIC_PREFIX + "health.stuckTasksCount", stuckTasksCountCopy::get));
    }
  }

  @Override
  public void applicationStarted() {
    leaderSelector.start();
  }

  @Override
  public void prepareForShutdown() {
    if (leaderSelector != null) {
      leaderSelector.stop();
    }
  }

  @Override
  public boolean canShutdown() {
    return leaderSelector == null || leaderSelector.hasStopped();
  }

  @Override
  public Integer getStuckTasksCount() {
    return stuckTasksCount == null ? null : stuckTasksCount.get();
  }

  @Override
  public List<Pair<String, Integer>> getErroneousTasksCountPerType() {
    return erroneousTasksCountPerType;
  }
}
