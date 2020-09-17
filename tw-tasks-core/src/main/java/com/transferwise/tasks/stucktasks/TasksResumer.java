package com.transferwise.tasks.stucktasks;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.TwContextClockHolder;
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
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.mdc.MdcContext;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.utils.DomainUtils;
import com.transferwise.tasks.utils.LogUtils;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
@RequiredArgsConstructor
public class TasksResumer implements ITasksResumer, GracefulShutdownStrategy {

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
  private IMeterHelper meterHelper;

  private LeaderSelectorV2 leaderSelector;

  private volatile boolean shuttingDown = false;
  // For tests.
  private volatile boolean paused;

  private final int batchSize = 1000;

  @PostConstruct
  public void init() {
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
  }

  /**
   * It's quite common for having zookeeper connection misconfigured and apps not do this check themselves, resulting in tasks resumer silently not
   * working at all.
   */
  @SuppressWarnings("checkstyle:magicnumber")
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

  @Trace(dispatcher = true)
  @SuppressWarnings("checkstyle:MultipleStringLiterals")
  protected void resumeStuckTasks(Leader.Control control) {
    NewRelic.setTransactionName("TwTasksEngine", "ResumeStuckTasks");
    try {
      List<TaskStatus> statusesToCheck = new ArrayList<>();
      statusesToCheck.add(TaskStatus.NEW);
      statusesToCheck.add(TaskStatus.SUBMITTED);
      statusesToCheck.add(TaskStatus.PROCESSING);

      while (statusesToCheck.size() > 0) {
        for (int i = statusesToCheck.size() - 1; i >= 0; i--) {
          ITaskDao.GetStuckTasksResponse result = taskDao.getStuckTasks(batchSize, statusesToCheck.get(i));
          AtomicInteger resumedCount = new AtomicInteger();
          AtomicInteger errorCount = new AtomicInteger();
          AtomicInteger failedCount = new AtomicInteger();

          for (ITaskDao.StuckTask task : result.getStuckTasks()) {
            if (control.shouldStop() || paused) {
              return;
            }

            MdcContext.with(() -> {
              MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
              handleStuckTask(task, resumedCount, errorCount, failedCount);
            });
          }
          log.debug("Resumed " + resumedCount + ", marked as error/failed " + errorCount + " / " + failedCount + " stuck tasks.");

          if (!result.isHasMore()) {
            statusesToCheck.remove(i);
          }
        }
      }
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  @Trace(dispatcher = true)
  @SuppressWarnings("checkstyle:MultipleStringLiterals")
  protected void resumeWaitingTasks(Leader.Control control) {
    NewRelic.setTransactionName("TwTasksEngine", "WaitingTasksResumer");
    try {
      while (true) {
        ITaskDao.GetStuckTasksResponse result = taskDao.getStuckTasks(batchSize, TaskStatus.WAITING);
        for (ITaskDao.StuckTask task : result.getStuckTasks()) {
          if (control.shouldStop() || paused) {
            return;
          }
          MdcContext.with(() -> {
            MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
            ZonedDateTime nextEventTime = taskHandlerRegistry.getExpectedProcessingMoment(task);
            if (!taskDao.markAsSubmitted(task.getVersionId().getId(), task.getVersionId().getVersion(), nextEventTime)) {
              log.debug("Were not able to mark task '" + task.getVersionId() + "' as submitted.");
              meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
            } else {
              BaseTask baseTask = DomainUtils.convert(task, BaseTask.class);
              baseTask.setVersion(baseTask.getVersion() + 1);

              tasksExecutionTriggerer.trigger(baseTask);
              meterHelper.registerScheduledTaskResuming(baseTask.getType());
            }
          });
        }
        if (log.isDebugEnabled() && result.getStuckTasks().size() > 0) {
          log.debug("Resumed " + result.getStuckTasks().size() + " waiting tasks.");
        }

        if (!result.isHasMore()) {
          break;
        }
      }
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  protected void handleStuckTask(ITaskDao.StuckTask task, AtomicInteger resumedCount, AtomicInteger errorCount, AtomicInteger failedCount) {
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
        taskResolutionStrategy = taskProcessingPolicy.getStuckTaskResolutionStrategy(task);
        bucketId = taskProcessingPolicy.getProcessingBucket(task);
      }
    }
    if (!TaskStatus.PROCESSING.name().equals(task.getStatus())) {
      retryTask(taskProcessingPolicy, bucketId, task, resumedCount);
      return;
    }

    if (taskResolutionStrategy == null) {
      log.error("No processing policy found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
      markTaskAsError(bucketId, task, errorCount);
      return;
    }

    switch (taskResolutionStrategy) {
      case RETRY:
        retryTask(taskProcessingPolicy, bucketId, task, resumedCount);
        break;
      case MARK_AS_ERROR:
        markTaskAsError(bucketId, task, errorCount);
        break;
      case MARK_AS_FAILED:
        if (!taskDao.setStatus(task.getVersionId().getId(), TaskStatus.FAILED, task.getVersionId().getVersion())) {
          meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.FAILED);
        } else {
          failedCount.getAndIncrement();
          String taskType = task.getType();
          meterHelper.registerStuckTaskMarkedAsFailed(taskType);
          meterHelper.registerTaskMarkedAsFailed(bucketId, taskType);
        }
        break;
      case IGNORE:
        meterHelper.registerStuckTaskAsIgnored(task.getType());
        break;
      default:
        throw new UnsupportedOperationException("Resolution strategy " + taskResolutionStrategy + " is not supported.");
    }
  }

  protected void retryTask(ITaskProcessingPolicy taskProcessingPolicy, String bucketId, ITaskDao.StuckTask task, AtomicInteger resumedCount) {
    if (!taskDao.markAsSubmitted(task.getVersionId().getId(), task.getVersionId().getVersion(), getMaxStuckTime(taskProcessingPolicy, task))) {
      meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
      return;
    }
    BaseTask baseTask = DomainUtils.convert(task, BaseTask.class);
    baseTask.setVersion(baseTask.getVersion() + 1); // markAsSubmittedAndSetNextEventTime is bumping task version, so we will as well.
    tasksExecutionTriggerer.trigger(baseTask);
    resumedCount.getAndIncrement();
    meterHelper.registerStuckTaskResuming(task.getType());
    meterHelper.registerTaskResuming(bucketId, task.getType());
  }

  protected void markTaskAsError(String bucketId, IBaseTask task, AtomicInteger errorCount) {
    log.error("Marking task " + LogUtils.asParameter(task.getVersionId()) + " as ERROR, because we don't know if it still processing somewhere.");
    if (!taskDao.setStatus(task.getVersionId().getId(), TaskStatus.ERROR, task.getVersionId().getVersion())) {
      meterHelper.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
    } else {
      errorCount.getAndIncrement();
      meterHelper.registerStuckTaskMarkedAsError(task.getType());
      meterHelper.registerTaskMarkedAsError(bucketId, task.getType());
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
    executorServicesProvider.getGlobalExecutorService().submit(this::resumeTasksForClient);
    leaderSelector.start();
  }

  @Trace(dispatcher = true)
  @SuppressWarnings("checkstyle:MultipleStringLiterals")
  protected void resumeTasksForClient() {
    NewRelic.setTransactionName("TwTasksEngine", "TasksForClientResuming");
    try {
      log.info("Checking if we can immediately resume this client's stuck tasks.");
      List<ITaskDao.StuckTask> clientTasks = taskDao
          .prepareStuckOnProcessingTasksForResuming(tasksProperties.getClientId(), getMaxStuckTime(null, null));
      for (ITaskDao.StuckTask task : clientTasks) {
        if (shuttingDown) {
          break;
        }
        MdcContext.with(() -> {
          MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
          log.info("Resuming client '" + tasksProperties.getClientId() + "' task " + LogUtils.asParameter(task.getVersionId())
              + " of type '" + task.getType() + "' in status '" + task.getStatus() + "'.");

          tasksExecutionTriggerer.trigger(DomainUtils.convert(task, BaseTask.class));

          meterHelper.registerStuckClientTaskResuming(task.getType());
          meterHelper.registerTaskResuming(null, task.getType());
        });
      }
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
}
