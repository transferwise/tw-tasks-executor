package com.transferwise.tasks.health;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.ILock;
import com.transferwise.common.leaderselector.LeaderSelectorV2;
import com.transferwise.common.leaderselector.SharedReentrantLockBuilderFactory;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.entrypoints.EntryPointsNames;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class ClusterWideTasksStateMonitor implements ITasksStateMonitor, GracefulShutdownStrategy, InitializingBean {

  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  LeaderSelectorV2 leaderSelector;

  private Map<String, Integer> erroneousTasksCountByType;
  private Map<String, AtomicInteger> erroneousTasksCounts;
  private AtomicInteger erroneousTasksCount;

  private AtomicInteger stuckTasksCount;
  private Map<Pair<TaskStatus, String>, Integer> stuckTasksCountByStatusAndType;
  private Map<Pair<TaskStatus, String>, AtomicInteger> stuckTasksCounts;

  private AtomicLong approximateTasksCount;
  private AtomicLong approximateUniqueKeysCount;
  private AtomicLong approximateTaskDatasCount;

  private Map<TaskStatus, AtomicLong> tasksHistoryLengthSeconds;

  private List<Object> registeredMetricHandles;
  private Map<String, Object> taskInErrorStateHandles;
  private Map<Pair<TaskStatus, String>, Object> stuckTasksStateHandles;

  private final Lock stateLock = new ReentrantLock();
  private boolean initialized;

  @Override
  public void afterPropertiesSet() {
    String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_state_monitor";

    ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-tsm", executorServicesProvider.getGlobalExecutorService());
    ILock lock = lockBuilderFactory.createBuilder(nodePath).build();
    leaderSelector = new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
      ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
      MutableObject<ScheduledTaskExecutor.TaskHandle> taskHandleHolder = new MutableObject<>();

      control.workAsyncUntilShouldStop(
          () -> {
            resetState(true);
            TasksProperties.ClusterWideTasksStateMonitor clusterWideTasksStateMonitor = tasksProperties.getClusterWideTasksStateMonitor();
            taskHandleHolder.setValue(scheduledTaskExecutor
                .scheduleAtFixedInterval(this::check, clusterWideTasksStateMonitor.getStartDelay(),
                    clusterWideTasksStateMonitor.getInterval()));
            log.info("Started to monitor tasks state for '" + tasksProperties.getGroupId() + "'.");
          },
          () -> {
            log.info("Stopping monitoring of tasks state for '" + tasksProperties.getGroupId() + "'.");
            if (taskHandleHolder.getValue() != null) {
              taskHandleHolder.getValue().stop();
              taskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
            }
            resetState(false);
            log.info("Monitoring of tasks state stopped.");
          });
    }).build();

    registerLibrary();

    log.info("Cluster-wide tasks state monitor initialized with lock key '{}'.", nodePath);
  }

  /**
   * Here we are fine with every node registering it's own library version.
   */
  protected void registerLibrary() {
    coreMetricsTemplate.registerLibrary();
  }

  @EntryPoint
  protected void resetState(boolean forInit) {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.MONITOR_RESET).toContext().execute(
        () -> {
          stateLock.lock();
          try {
            /*
              The main idea between unregistering the metrics, is to not left 0 or old values lying around in Grafana but make this metric disappear
              from current node.
              This will make the picture much more clear and accurate.
            */
            if (registeredMetricHandles != null) {
              for (Object metricHandle : registeredMetricHandles) {
                coreMetricsTemplate.unregisterMetric(metricHandle);
              }
            }

            approximateTasksCount = null;
            approximateUniqueKeysCount = null;
            approximateTaskDatasCount = null;

            stuckTasksCount = null;
            stuckTasksCounts = new HashMap<>();
            stuckTasksCountByStatusAndType = new HashMap<>();

            erroneousTasksCount = null;
            erroneousTasksCounts = new HashMap<>();
            erroneousTasksCountByType = new HashMap<>();
            registeredMetricHandles = new ArrayList<>();
            taskInErrorStateHandles = new HashMap<>();
            stuckTasksStateHandles = new HashMap<>();
            tasksHistoryLengthSeconds = new HashMap<>();

            initialized = forInit;
          } finally {
            stateLock.unlock();
          }
        });
  }

  @EntryPoint
  protected void check() {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.MONITOR_CHECK).toContext().execute(
        () -> {
          stateLock.lock();
          try {
            if (!initialized) {
              return;
            }

            checkErroneousTasks();
            checkStuckTasks();
            measureTasksHistoryLength();
            if (tasksProperties.getClusterWideTasksStateMonitor().isTasksCountingEnabled()) {
              checkApproximateTasksCount();
              checkApproximateUniqueKeysCount();
              checkApproximateTaskDatasCount();
            }
          } finally {
            stateLock.unlock();
          }
        });
  }

  protected void measureTasksHistoryLength() {
    measureTaskHistoryLength(TaskStatus.DONE);
    measureTaskHistoryLength(TaskStatus.ERROR);
  }

  protected void measureTaskHistoryLength(TaskStatus status) {
    ZonedDateTime now = ZonedDateTime.now(TwContextClockHolder.getClock());
    ZonedDateTime earliestTaskNextEventTime = taskDao.getEarliestTaskNextEventTime(status);
    long historyLengthSeconds = earliestTaskNextEventTime == null ? 0L : Duration.between(earliestTaskNextEventTime, now).getSeconds();

    AtomicLong counter = tasksHistoryLengthSeconds.get(status);
    if (counter == null) {
      tasksHistoryLengthSeconds.put(status, counter = new AtomicLong(historyLengthSeconds));
      registeredMetricHandles
          .add(coreMetricsTemplate.registerTaskHistoryLength(status, counter));
    } else {
      counter.set(historyLengthSeconds);
    }
  }

  protected void checkErroneousTasks() {
    int tasksCountInError = taskDao.getTasksCountInStatus(tasksProperties.getMaxDatabaseFetchSize(), TaskStatus.ERROR);
    if (tasksCountInError == 0) {
      erroneousTasksCountByType = Collections.emptyMap();
    } else {
      erroneousTasksCountByType = taskDao.getErronousTasksCountByType(tasksProperties.getMaxDatabaseFetchSize());
    }

    if (erroneousTasksCount == null) {
      erroneousTasksCount = new AtomicInteger(tasksCountInError);
      registeredMetricHandles.add(coreMetricsTemplate.registerTasksInErrorCount(erroneousTasksCount));
    } else {
      erroneousTasksCount.set(tasksCountInError);
    }

    Set<String> erroneousTaskTypes = new HashSet<>();
    erroneousTasksCountByType.forEach((type, count) -> {
      erroneousTaskTypes.add(type);
      AtomicInteger typeCounter = erroneousTasksCounts.computeIfAbsent(type, k -> {
        AtomicInteger cnt = new AtomicInteger();
        Object handle = coreMetricsTemplate.registerTasksInErrorCount(type, cnt);
        registeredMetricHandles.add(handle);
        taskInErrorStateHandles.put(type, handle);
        return cnt;
      });
      typeCounter.set(count);
    });

    // make sure that we reset values for the tasks that are not in error state anymore
    for (Iterator<String> it = erroneousTasksCounts.keySet().iterator(); it.hasNext(); ) {
      String taskType = it.next();
      if (!erroneousTaskTypes.contains(taskType)) {
        Object handle = taskInErrorStateHandles.remove(taskType);
        registeredMetricHandles.remove(handle);
        coreMetricsTemplate.unregisterMetric(handle);
        it.remove();
      }
    }
  }

  protected void checkStuckTasks() {
    ZonedDateTime age = ZonedDateTime.now(TwContextClockHolder.getClock()).minus(tasksProperties.getStuckTaskAge());
    int stuckTasksCountValue = taskDao.getStuckTasksCount(age, tasksProperties.getMaxDatabaseFetchSize());

    if (stuckTasksCountValue == 0) {
      stuckTasksCountByStatusAndType = Collections.emptyMap();
    } else {
      stuckTasksCountByStatusAndType = taskDao.getStuckTasksCountByStatusAndType(age, tasksProperties.getMaxDatabaseFetchSize());
    }

    if (stuckTasksCount == null) {
      stuckTasksCount = new AtomicInteger(stuckTasksCountValue);
      registeredMetricHandles.add(coreMetricsTemplate.registerStuckTasksCount(stuckTasksCount));
    } else {
      stuckTasksCount.set(stuckTasksCountValue);
    }

    Set<Pair<TaskStatus, String>> stuckTasksByStatusAndType = new HashSet<>();
    stuckTasksCountByStatusAndType.forEach((statusAndType, count) -> {
      stuckTasksByStatusAndType.add(statusAndType);
      AtomicInteger typeCounter = stuckTasksCounts.computeIfAbsent(statusAndType, k -> {
        AtomicInteger cnt = new AtomicInteger();
        Object handle = coreMetricsTemplate.registerStuckTasksCount(statusAndType.getLeft(), statusAndType.getRight(), cnt);
        registeredMetricHandles.add(handle);
        stuckTasksStateHandles.put(statusAndType, handle);
        return cnt;
      });
      typeCounter.set(count);
    });

    // make sure that we reset values for the tasks that are not stuck anymore
    for (Iterator<Pair<TaskStatus, String>> it = stuckTasksCounts.keySet().iterator(); it.hasNext(); ) {
      Pair<TaskStatus, String> taskStatusAndType = it.next();
      if (!stuckTasksByStatusAndType.contains(taskStatusAndType)) {
        Object handle = stuckTasksStateHandles.remove(taskStatusAndType);
        registeredMetricHandles.remove(handle);
        coreMetricsTemplate.unregisterMetric(handle);
        it.remove();
      }
    }
  }

  protected void checkApproximateTasksCount() {
    long approximateTasksCountValue = taskDao.getApproximateTasksCount();

    if (approximateTasksCount == null) {
      approximateTasksCount = new AtomicLong(approximateTasksCountValue);
      registeredMetricHandles.add(coreMetricsTemplate.registerApproximateTasksCount(approximateTasksCount));
    } else {
      approximateTasksCount.set(approximateTasksCountValue);
    }
  }

  protected void checkApproximateUniqueKeysCount() {
    long approximateUniqueKeysCountValue = taskDao.getApproximateUniqueKeysCount();

    if (approximateUniqueKeysCount == null) {
      approximateUniqueKeysCount = new AtomicLong(approximateUniqueKeysCountValue);
      registeredMetricHandles.add(coreMetricsTemplate.registerApproximateUniqueKeysCount(approximateUniqueKeysCount));
    } else {
      approximateUniqueKeysCount.set(approximateUniqueKeysCountValue);
    }
  }

  protected void checkApproximateTaskDatasCount() {
    long approximateTaskDatasCountValue = taskDao.getApproximateTaskDatasCount();

    if (approximateTaskDatasCount == null) {
      approximateTaskDatasCount = new AtomicLong(approximateTaskDatasCountValue);
      registeredMetricHandles.add(coreMetricsTemplate.registerApproximateTaskDatasCount(approximateTaskDatasCount));
    } else {
      approximateTaskDatasCount.set(approximateTaskDatasCountValue);
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
  public Map<String, Integer> getErroneousTasksCountByType() {
    return erroneousTasksCountByType;
  }

  @Override
  public Map<Pair<TaskStatus, String>, Integer> getStuckTasksCountByType() {
    return stuckTasksCountByStatusAndType;
  }
}
