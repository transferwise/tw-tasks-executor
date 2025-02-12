package com.transferwise.tasks.cleaning;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TasksCleaner implements ITasksCleaner, GracefulShutdownStrategy, InitializingBean {

  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  private LeaderSelectorV2 leaderSelector;

  private final List<DeletableStatus> deletableStatuses = new ArrayList<>();

  @Override
  public void afterPropertiesSet() {
    String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_cleaner";

    TaskStatus[] statuses = {TaskStatus.DONE, TaskStatus.FAILED, TaskStatus.CANCELLED};
    for (TaskStatus status : statuses) {
      DeletableStatus deletableStatus = new DeletableStatus();
      deletableStatus.status = status;
      deletableStatuses.add(deletableStatus);
    }

    ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-cleaner", executorServicesProvider.getGlobalExecutorService());
    ILock lock = lockBuilderFactory.createBuilder(nodePath).build();

    leaderSelector = new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
      ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
      MutableObject<ScheduledTaskExecutor.TaskHandle> taskHandleHolder = new MutableObject<>();

      control.workAsyncUntilShouldStop(
          () -> {
            taskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(this::deleteFinishedOldTasks,
                tasksProperties.getTasksCleaningInterval(),
                tasksProperties.getTasksCleaningInterval()));
            log.info("Started to clean finished tasks older than " + tasksProperties.getFinishedTasksHistoryToKeep() + " for '" + tasksProperties
                .getGroupId() + "'.");
          },
          () -> {
            log.info("Stopping tasks cleaner " + " for '" + tasksProperties.getGroupId() + "'.");
            if (taskHandleHolder.getValue() != null) {
              taskHandleHolder.getValue().stop();
              taskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
            }
            for (DeletableStatus deletableStatus : deletableStatuses) {
              // We don't want old values, 0-s or -1s to be shown in Grafana when we are not even doing any work.
              if (deletableStatus.metricHandle != null) {
                coreMetricsTemplate.unregisterMetric(deletableStatus.metricHandle);
                deletableStatus.metricHandle = null;
              }
            }
            log.info("Tasks cleaner stopped.");
          });
    }).build();

    log.info("Tasks cleaner initialized with lock key '{}'.", nodePath);
  }

  @EntryPoint
  protected void deleteFinishedOldTasks() {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.CLEAN_OLD_TAKS).toContext()
        .execute(() -> {
          for (DeletableStatus deletableStatus : deletableStatuses) {
            try {
              TaskStatus status = deletableStatus.status;
              ITaskDao.DeleteFinishedOldTasksResult result = taskDao
                  .deleteOldTasks(status, tasksProperties.getFinishedTasksHistoryToKeep(), tasksProperties.getTasksHistoryDeletingBatchSize());

              coreMetricsTemplate.registerTasksCleanerTasksDeletion(status, result.getFoundTasksCount(), result.getDeletedTasksCount(),
                  result.getDeletedUniqueKeysCount(), result.getDeletedTaskDatasCount());

              long lagSeconds = result.getFirstDeletedTaskNextEventTime() == null ? 0 :
                  Duration.between(result.getFirstDeletedTaskNextEventTime(), result.getDeletedBeforeTime()).getSeconds();

              boolean lagNotRegistered = deletableStatus.lagSeconds == null;
              if (lagNotRegistered) {
                deletableStatus.lagSeconds = new AtomicLong(lagSeconds);
              } else {
                deletableStatus.lagSeconds.set(lagSeconds);
              }
              if (lagNotRegistered) {
                AtomicLong lagSecondsCopy = deletableStatus.lagSeconds;
                deletableStatus.metricHandle = coreMetricsTemplate.registerTasksCleanerDeleteLagSeconds(deletableStatus.status, lagSecondsCopy);
              }

              if (log.isDebugEnabled()) {
                log.debug(
                    "Deleted finished old tasks for status " + status.name() + ". Found: " + result.getFoundTasksCount() + ", deleted: " + result
                        .getDeletedTasksCount() + ", deleted unique keys: " + result.getDeletedUniqueKeysCount()
                        + ", deleted task datas: " + result.getDeletedTaskDatasCount()
                        + ". First task was '" + result.getFirstDeletedTaskId() + "':'" + result.getFirstDeletedTaskNextEventTime()
                        + "', time barrier was '"
                        + result
                        .getDeletedBeforeTime() + "'.");
              }
            } catch (Throwable t) {
              log.error(t.getMessage(), t);
            }
          }
        });
  }

  @Override
  public void applicationStarted() {
    if (tasksProperties.isStartTasksCleaner()) {
      leaderSelector.start();
    }
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

  private static class DeletableStatus {

    TaskStatus status;
    AtomicLong lagSeconds;
    Object metricHandle;
  }
}
