package com.transferwise.tasks.stucktasks;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.ILock;
import com.transferwise.common.leaderselector.Leader;
import com.transferwise.common.leaderselector.LeaderSelectorV2;
import com.transferwise.common.leaderselector.SharedReentrantLockBuilderFactory;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.entrypoints.EntryPointsNames;
import com.transferwise.tasks.entrypoints.IMdcService;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
import com.transferwise.tasks.processing.ITasksProcessingService;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.utils.DomainUtils;
import com.transferwise.tasks.utils.LogUtils;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@RequiredArgsConstructor
public class TasksResumer implements ITasksResumer, GracefulShutdownStrategy, InitializingBean {

  @Autowired
  private ITasksExecutionTriggerer tasksExecutionTriggerer;
  @Autowired
  private ITaskHandlerRegistry taskHandlerRegistry;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private CuratorFramework curatorFramework;
  @Autowired
  private SharedReentrantLockBuilderFactory lockBuilderFactory;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private IMdcService mdcService;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private ITasksProcessingService tasksProcessingService;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  private LeaderSelectorV2 leaderSelector;

  private volatile boolean shuttingDown = false;
  // For tests.
  private volatile boolean paused;

  @Override
  public void afterPropertiesSet() {
    String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_resumer";
    ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-resumer", executorServicesProvider.getGlobalExecutorService());

    verifyCorrectCuratorConfig();

    ILock lock = lockBuilderFactory.createBuilder(nodePath).build();
    leaderSelector = new LeaderSelectorV2.Builder().setLock(lock).setExecutorService(executorService).setLeader(control -> {
      ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
      MutableObject<ScheduledTaskExecutor.TaskHandle> stuckTaskHandleHolder = new MutableObject<>();
      MutableObject<ScheduledTaskExecutor.TaskHandle> scheduledTaskHandleHolder = new MutableObject<>();

      control.workAsyncUntilShouldStop(
          () -> {
            stuckTaskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(() -> resumeStuckTasks(control),
                tasksProperties.getStuckTasksPollingInterval(), tasksProperties.getStuckTasksPollingInterval()));

            log.info("Started to resume stuck tasks after each " + tasksProperties.getStuckTasksPollingInterval() + " for '"
                + tasksProperties.getGroupId() + "'.");

            scheduledTaskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(() -> {
              if (paused) {
                return;
              }
              resumeWaitingTasks(control);
            }, tasksProperties.getWaitingTasksPollingInterval(), tasksProperties.getWaitingTasksPollingInterval()));

            log.info("Started to resume scheduled tasks after each " + tasksProperties.getWaitingTasksPollingInterval() + " for '"
                + tasksProperties.getGroupId() + "'.");
          },
          () -> {
            log.info("Stopping stuck tasks resumer for '" + tasksProperties.getGroupId() + "'.");
            if (stuckTaskHandleHolder.getValue() != null) {
              stuckTaskHandleHolder.getValue().stop();
            }

            log.info("Stopping scheduled tasks executor for '" + tasksProperties.getGroupId() + "'.");
            if (scheduledTaskHandleHolder.getValue() != null) {
              scheduledTaskHandleHolder.getValue().stop();
            }

            if (stuckTaskHandleHolder.getValue() != null) {
              stuckTaskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
            }
            log.info("Stuck tasks resumer stopped for '" + tasksProperties.getGroupId() + "'.");
            if (scheduledTaskHandleHolder.getValue() != null) {
              scheduledTaskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
            }
            log.info("Scheduled tasks executor stopped '" + tasksProperties.getGroupId() + "'.");
          });
    }).build();

    log.info("Tasks resumer initialized with lock key '{}'.", nodePath);
  }

  /**
   * It's quite common for having zookeeper connection misconfigured and apps not do this check themselves, resulting in tasks resumer silently not
   * working at all.
   */
  protected void verifyCorrectCuratorConfig() {
    if (!tasksProperties.isPreventStartWithoutZookeeper()) {
      return;
    }
    ExceptionUtils.doUnchecked(() -> {
      while (!curatorFramework.blockUntilConnected(5, TimeUnit.SECONDS)) {
        log.error("Connection to Zookeeper at '{}' failed.",
            curatorFramework.getZookeeperClient().getCurrentConnectionString());
      }
    });
  }

  @EntryPoint
  protected void resumeStuckTasks(Leader.Control control) {
    try {
      unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.RESUME_STUCK_TASKS).toContext()
          .execute(() -> {
            List<TaskStatus> statusesToCheck = new ArrayList<>();
            statusesToCheck.add(TaskStatus.NEW);
            statusesToCheck.add(TaskStatus.SUBMITTED);
            statusesToCheck.add(TaskStatus.PROCESSING);

            var executorService = new ThreadNamingExecutorServiceWrapper("stk-tasks-resumer", executorServicesProvider.getGlobalExecutorService());
            int concurrency = tasksProperties.getTasksResumer().getConcurrency();
            var semaphore = new Semaphore(concurrency);
            var futures = new ArrayList<Future<Boolean>>(concurrency);
            var waitTimeMs = tasksProperties.getGenericMediumDelay().toMillis();

            while (statusesToCheck.size() > 0) {
              for (int i = statusesToCheck.size() - 1; i >= 0; i--) {
                ITaskDao.GetStuckTasksResponse result = taskDao.getStuckTasks(tasksProperties.getTasksResumer().getBatchSize(),
                    statusesToCheck.get(i));
                StuckTaskResolutionStats stuckTaskResolutionStats = new StuckTaskResolutionStats();

                for (ITaskDao.StuckTask task : result.getStuckTasks()) {
                  var gotLease = false;

                  while (!gotLease) {
                    if (control.shouldStop() || paused) {
                      break;
                    }
                    gotLease = ExceptionUtils.doUnchecked(() -> semaphore.tryAcquire(waitTimeMs, TimeUnit.MILLISECONDS));
                  }

                  if (gotLease) {
                    futures.add(executorService.submit(() -> {
                      try {
                        mdcService.put(task);
                        handleStuckTask(task, StuckDetectionSource.CLUSTER_WIDE_STUCK_TASKS_DETECTOR, stuckTaskResolutionStats);
                      } catch (Throwable t) {
                        log.error("Resuming stuck task '" + task.getVersionId() + "' failed.", t);
                        return false;
                      } finally {
                        semaphore.release();
                      }
                      return true;
                    }));
                  }
                }

                long startTimeMs = TwContextClockHolder.getClock().millis();

                var successCnt = new MutableInt();
                // This algorithm can create a bit tail lag, but on the other hand allows to create it relatively simple and robust.
                for (var f : futures) {
                  if (TwContextClockHolder.getClock().millis() - startTimeMs > waitTimeMs) {
                    log.error("Stall detected in resuming scheduled tasks.");
                    break;
                  }
                  ExceptionUtils.doUnchecked(() -> {
                    if (Boolean.TRUE.equals(f.get(waitTimeMs, TimeUnit.MILLISECONDS))) {
                      successCnt.increment();
                    }
                  });
                }

                stuckTaskResolutionStats.logStats();

                if (successCnt.getValue() < futures.size()) {
                  ExceptionUtils.doUnchecked(() -> Thread.sleep(tasksProperties.getGenericMediumDelay().toMillis()));
                }

                if (!result.isHasMore()) {
                  statusesToCheck.remove(i);
                }

                futures.clear();
              }
            }
          });
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  @EntryPoint
  protected void resumeWaitingTasks(Leader.Control control) {
    try {
      unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.RESUME_WAITING_TASKS)
          .toContext()
          .execute(() -> {

            var waitTimeMs = tasksProperties.getGenericMediumDelay().toMillis();
            var executorService = new ThreadNamingExecutorServiceWrapper("sch-tasks-resumer", executorServicesProvider.getGlobalExecutorService());
            var concurrency = tasksProperties.getTasksResumer().getConcurrency();
            var semaphore = new Semaphore(concurrency);
            var futures = new ArrayList<Future<Boolean>>(concurrency);

            while (true) {
              var result = taskDao.getStuckTasks(tasksProperties.getTasksResumer().getBatchSize(), TaskStatus.WAITING);
              for (var task : result.getStuckTasks()) {
                var gotLease = false;

                while (!gotLease) {
                  if (control.shouldStop() || paused) {
                    break;
                  }
                  gotLease = ExceptionUtils.doUnchecked(
                      () -> semaphore.tryAcquire(waitTimeMs, TimeUnit.MILLISECONDS));
                }

                if (gotLease) {
                  futures.add(executorService.submit(() -> {
                    try {
                      mdcService.put(task);
                      var nextEventTime = taskHandlerRegistry.getExpectedProcessingMoment(task);
                      if (!taskDao.markAsSubmitted(task.getVersionId().getId(), task.getVersionId().getVersion(), nextEventTime)) {
                        if (log.isDebugEnabled()) {
                          log.debug("Were not able to mark task '" + task.getVersionId() + "' as submitted.");
                        }
                        coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
                      } else {
                        var baseTask = DomainUtils.convert(task, BaseTask.class);
                        baseTask.setVersion(baseTask.getVersion() + 1);

                        tasksExecutionTriggerer.trigger(baseTask);
                        coreMetricsTemplate.registerScheduledTaskResuming(baseTask.getType());
                      }
                    } catch (Throwable t) {
                      log.error("Resuming scheduled task '" + task.getVersionId() + "' failed.", t);
                      return false;
                    } finally {
                      semaphore.release();
                    }
                    return true;
                  }));
                }
              }

              long startTimeMs = TwContextClockHolder.getClock().millis();

              var successCnt = new MutableInt();
              // This algorithm can create a bit tail lag, but on the other hand allows to create it relatively simple and robust.
              for (var f : futures) {
                if (TwContextClockHolder.getClock().millis() - startTimeMs > waitTimeMs) {
                  log.error("Stall detected in resuming scheduled tasks.");
                  break;
                }
                ExceptionUtils.doUnchecked(() -> {
                  if (Boolean.TRUE.equals(f.get(waitTimeMs, TimeUnit.MILLISECONDS))) {
                    successCnt.increment();
                  }
                });
              }

              if (log.isDebugEnabled() && result.getStuckTasks().size() > 0) {
                log.debug("Resumed " + result.getStuckTasks().size() + " waiting tasks.");
              }

              if (successCnt.getValue() < futures.size()) {
                ExceptionUtils.doUnchecked(() -> Thread.sleep(tasksProperties.getGenericMediumDelay().toMillis()));
              }

              if (!result.isHasMore()) {
                break;
              }
              futures.clear();
            }
          });
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  protected void handleStuckTask(ITaskDao.StuckTask task, StuckDetectionSource stuckDetectionSource,
      StuckTaskResolutionStats stuckTaskResolutionStats) {
    ITaskProcessingPolicy.StuckTaskResolutionStrategy taskResolutionStrategy = null;

    ITaskHandler taskHandler = taskHandlerRegistry.getTaskHandler(task);

    String bucketId = null;
    ITaskProcessingPolicy taskProcessingPolicy = null;
    if (taskHandler == null) {
      log.error("No task handler found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
    } else {
      taskProcessingPolicy = taskHandler.getProcessingPolicy(task);
      if (taskProcessingPolicy == null) {
        log.error("No processing policy found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
      } else {
        taskResolutionStrategy = taskProcessingPolicy.getStuckTaskResolutionStrategy(task, stuckDetectionSource);
        bucketId = taskProcessingPolicy.getProcessingBucket(task);
      }
    }
    if (!TaskStatus.PROCESSING.name().equals(task.getStatus())) {
      retryTask(taskProcessingPolicy, bucketId, task, stuckDetectionSource, stuckTaskResolutionStats);
      return;
    }

    if (taskResolutionStrategy == null) {
      log.error("No processing policy found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
      markTaskAsError(bucketId, task, stuckDetectionSource, stuckTaskResolutionStats);
      return;
    }

    switch (taskResolutionStrategy) {
      case RETRY:
        retryTask(taskProcessingPolicy, bucketId, task, stuckDetectionSource, stuckTaskResolutionStats);
        break;
      case MARK_AS_ERROR:
        markTaskAsError(bucketId, task, stuckDetectionSource, stuckTaskResolutionStats);
        break;
      case MARK_AS_FAILED:
        if (!taskDao.setStatus(task.getVersionId().getId(), TaskStatus.FAILED, task.getVersionId().getVersion())) {
          coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.FAILED);
        } else {
          stuckTaskResolutionStats.countFailed();
          String taskType = task.getType();
          coreMetricsTemplate.registerStuckTaskMarkedAsFailed(taskType, stuckDetectionSource);
          coreMetricsTemplate.registerTaskMarkedAsFailed(bucketId, taskType);
        }
        break;
      case IGNORE:
        coreMetricsTemplate.registerStuckTaskAsIgnored(task.getType(), stuckDetectionSource);
        break;
      default:
        throw new UnsupportedOperationException("Resolution strategy " + taskResolutionStrategy + " is not supported.");
    }
  }

  protected void retryTask(ITaskProcessingPolicy taskProcessingPolicy, String bucketId, ITaskDao.StuckTask task,
      StuckDetectionSource stuckDetectionSource, StuckTaskResolutionStats stuckTaskResolutionStats) {
    if (!taskDao.markAsSubmitted(task.getVersionId().getId(), task.getVersionId().getVersion(), getMaxStuckTime(taskProcessingPolicy, task))) {
      coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
      return;
    }
    BaseTask baseTask = DomainUtils.convert(task, BaseTask.class);
    baseTask.setVersion(baseTask.getVersion() + 1); // markAsSubmittedAndSetNextEventTime is bumping task version, so we will as well.
    tasksExecutionTriggerer.trigger(baseTask);
    stuckTaskResolutionStats.countResumed();
    coreMetricsTemplate.registerStuckTaskResuming(task.getType(), stuckDetectionSource);
    coreMetricsTemplate.registerTaskResuming(bucketId, task.getType());
  }

  protected void markTaskAsError(String bucketId, IBaseTask task, StuckDetectionSource stuckDetectionSource,
      StuckTaskResolutionStats stuckTaskResolutionStats) {
    log.error("Marking task " + LogUtils.asParameter(task.getVersionId()) + " as ERROR, because we don't know if it still processing somewhere.");
    if (!taskDao.setStatus(task.getVersionId().getId(), TaskStatus.ERROR, task.getVersionId().getVersion())) {
      coreMetricsTemplate.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
    } else {
      stuckTaskResolutionStats.countError();
      coreMetricsTemplate.registerStuckTaskMarkedAsError(task.getType(), stuckDetectionSource);
      coreMetricsTemplate.registerTaskMarkedAsError(bucketId, task.getType());
    }
  }

  public void pauseProcessing() {
    paused = true;
  }

  public void resumeProcessing() {
    paused = false;
  }

  private ZonedDateTime getMaxStuckTime(ITaskProcessingPolicy taskProcessingPolicy, IBaseTask baseTask) {
    Duration timeout = taskProcessingPolicy != null ? taskProcessingPolicy.getExpectedQueueTime(baseTask) : null;
    if (timeout == null) {
      timeout = tasksProperties.getTaskStuckTimeout();
    }
    return ZonedDateTime.now(TwContextClockHolder.getClock()).plus(timeout);
  }

  @Override
  public void applicationStarted() {
    if (tasksProperties.isAutoStartProcessing()) {
      executorServicesProvider.getGlobalExecutorService().submit(this::resumeTasksForClient);
      leaderSelector.start();
    }
  }

  @EntryPoint
  protected void resumeTasksForClient() {
    try {
      unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.RESUME_TASKS_FOR_CLIENT).toContext()
          .execute(() -> {
            try {
              final StuckTaskResolutionStats stuckTaskResolutionStats = new StuckTaskResolutionStats();

              log.info("Checking if we can immediately resume this client's stuck tasks.");
              // We don't split to batches here, because the amount of `PROCESSING` tasks is usually limited by concurrency policies.
              List<ITaskDao.StuckTask> clientTasks = taskDao
                  .prepareStuckOnProcessingTasksForResuming(tasksProperties.getClientId(), getMaxStuckTime(null, null));

              if (!shuttingDown) {
                // We can only start processing here. If we start processing earlier, the query above can find tasks just marked as PROCESSING
                // by the same node we are running on.
                tasksProcessingService.startProcessing();
              }

              for (ITaskDao.StuckTask task : clientTasks) {
                if (shuttingDown) {
                  break;
                }
                mdcService.put(task);
                log.info("Found client '" + tasksProperties.getClientId() + "' task " + LogUtils.asParameter(task.getVersionId())
                    + " of type '" + task.getType() + "' stuck in status '" + task.getStatus() + "'.");

                handleStuckTask(task, StuckDetectionSource.SAME_NODE_STARTUP, stuckTaskResolutionStats);
              }
            } catch (Throwable t) {
              log.error(t.getMessage(), t);
            } finally {
              if (!shuttingDown) {
                // If just this resume operation fails, we are still willing to start the engine.
                tasksProcessingService.startProcessing();
              }
            }
          });
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  @Override
  public void prepareForShutdown() {
    shuttingDown = true;
    leaderSelector.stop();
  }

  @Override
  public boolean canShutdown() {
    return leaderSelector.hasStopped();
  }

  @Data
  @Accessors(chain = true)
  public static class StuckTaskResolutionStats {

    private AtomicInteger failed = new AtomicInteger();
    private AtomicInteger resumed = new AtomicInteger();
    private AtomicInteger error = new AtomicInteger();

    private void countFailed() {
      failed.incrementAndGet();
    }

    private void countResumed() {
      resumed.incrementAndGet();
    }

    private void countError() {
      error.incrementAndGet();
    }

    private boolean hasStats() {
      return failed.get() > 0 || resumed.get() > 0 || error.get() > 0;
    }

    private void logStats() {
      if (log.isDebugEnabled() && hasStats()) {
        log.debug("Resumed " + resumed + ", marked as error/failed " + error + " / " + failed + " stuck tasks.");
      }
    }
  }
}
