package com.transferwise.tasks.stucktasks;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.Leader;
import com.transferwise.common.leaderselector.LeaderSelector;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.config.IExecutorServicesProvider;
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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.curator.framework.CuratorFramework;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.transferwise.tasks.helpers.IMeterHelper.METRIC_PREFIX;

@Slf4j
@RequiredArgsConstructor
public class TasksResumer implements ITasksResumer, GracefulShutdownStrategy {
    private final ITasksExecutionTriggerer tasksExecutionTriggerer;
    private final ITaskHandlerRegistry taskHandlerRegistry;
    private final TasksProperties tasksProperties;
    private final ITaskDao taskDao;
    private final CuratorFramework curatorFramework;
    private final IExecutorServicesProvider executorServicesProvider;
    private final IMeterHelper meterHelper;

    private LeaderSelector leaderSelector;

    private volatile boolean shuttingDown = false;
    // For tests.
    private volatile boolean paused;

    @SuppressWarnings("checkstyle:magicnumber")
    private int batchSize = 1000;

    @PostConstruct
    @SuppressWarnings({"checkstyle:magicnumber", "checkstyle:MultipleStringLiterals"})
    public void init() {
        String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_resumer";
        ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-resumer", executorServicesProvider.getGlobalExecutorService());

        verifyCorrectCuratorConfig();

        leaderSelector = new LeaderSelector(curatorFramework, nodePath, executorService,
            control -> {
                ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
                MutableObject<ScheduledTaskExecutor.TaskHandle> stuckTaskHandleHolder = new MutableObject<>();
                MutableObject<ScheduledTaskExecutor.TaskHandle> scheduledTaskHandleHolder = new MutableObject<>();

                control.workAsyncUntilShouldStop(
                    () -> {
                        stuckTaskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(() -> resumeStuckTasks(control),
                            tasksProperties.getStuckTasksPollingInterval(), tasksProperties.getStuckTasksPollingInterval()));

                        log.info("Started to resume stuck tasks after each " + tasksProperties.getStuckTasksPollingInterval() + ".");

                        scheduledTaskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(() -> {
                            if (paused) {
                                return;
                            }
                            resumeWaitingTasks(control);
                        }, tasksProperties.getWaitingTasksPollingInterval(), tasksProperties.getWaitingTasksPollingInterval()));

                        log.info("Started to resume scheduled tasks after each " + tasksProperties.getWaitingTasksPollingInterval() + ".");
                    },
                    () -> {
                        log.info("Stopping stuck tasks resumer.");
                        if (stuckTaskHandleHolder.getValue() != null) {
                            stuckTaskHandleHolder.getValue().stop();
                        }

                        log.info("Stopping scheduled tasks executor.");
                        if (scheduledTaskHandleHolder.getValue() != null) {
                            scheduledTaskHandleHolder.getValue().stop();
                        }

                        if (stuckTaskHandleHolder.getValue() != null) {
                            stuckTaskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
                        }
                        log.info("Stuck tasks resumer stopped.");
                        if (scheduledTaskHandleHolder.getValue() != null) {
                            scheduledTaskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
                        }
                        log.info("Scheduled tasks executor stopped.");
                    });
            });
    }

    /**
     * It's quite common for having zookeeper connection misconfigured and apps not do this check themselves,
     * resulting in tasks resumer silently not working at all.
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
            while (true) {
                ITaskDao.GetStuckTasksResponse result = taskDao.getStuckTasks(batchSize, TaskStatus.NEW,
                    TaskStatus.SUBMITTED, TaskStatus.PROCESSING);
                AtomicInteger nResumed = new AtomicInteger();
                AtomicInteger nError = new AtomicInteger();
                AtomicInteger nFailed = new AtomicInteger();

                for (ITaskDao.StuckTask task : result.getStuckTasks()) {
                    if (control.shouldStop()) {
                        return;
                    }

                    MdcContext.with(() -> {
                        MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
                        handleStuckTask(task, nResumed, nError, nFailed);
                    });
                }
                log.debug("Resumed " + nResumed + ", marked as error/failed " + nError + " / " + nFailed + " stuck tasks.");

                if (!result.isHasMore()) {
                    break;
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
                    if (control.shouldStop()) {
                        return;
                    }
                    MdcContext.with(() -> {
                        MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
                        taskDao.setStatus(task.getVersionId().getId(), TaskStatus.SUBMITTED, task.getVersionId().getVersion());

                        BaseTask baseTask = DomainUtils.convert(task, BaseTask.class);
                        baseTask.setVersion(baseTask.getVersion() + 1);

                        tasksExecutionTriggerer.trigger(baseTask);
                        meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.scheduledTasks.resumedCount", 1);
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

    protected void handleStuckTask(ITaskDao.StuckTask task, AtomicInteger nResumed, AtomicInteger nError, AtomicInteger nFailed) {
        ITaskProcessingPolicy.StuckTaskResolutionStrategy strategy = null;

        if (!TaskStatus.PROCESSING.name().equals(task.getStatus())) {
            retryTask(task, nResumed);
            return;
        }

        ITaskHandler taskHandler = taskHandlerRegistry.getTaskHandler(task);
        if (taskHandler == null) {
            log.error("No task handler found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
        } else {
            ITaskProcessingPolicy taskProcessingPolicy = taskHandler.getProcessingPolicy(task);
            if (taskProcessingPolicy == null) {
                log.error("No processing policy found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
            } else {
                strategy = taskProcessingPolicy.getStuckTaskResolutionStrategy(task);
            }
        }
        if (strategy == null) {
            log.error("No processing policy found for task " + LogUtils.asParameter(task.getVersionId()) + ".");
            markTaskAsError(task, nError);
            return;
        }

        ITaskProcessingPolicy.StuckTaskResolutionStrategy resolutionStrategy = taskHandlerRegistry
            .getTaskHandler(task).getProcessingPolicy(task).getStuckTaskResolutionStrategy(task);
        switch (resolutionStrategy) {
            case RETRY:
                retryTask(task, nResumed);
                break;
            case MARK_AS_ERROR:
                markTaskAsError(task, nError);
                break;
            case MARK_AS_FAILED:
                taskDao.setStatus(task.getVersionId().getId(), TaskStatus.FAILED, task.getVersionId().getVersion());
                nFailed.getAndIncrement();
                meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.stuckTasks.markFailedCount", 1);
                meterHelper.registerTaskMarkedAsFailed(null, task.getType());
                break;
            case IGNORE:
                meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.stuckTasks.ignoredCount", 1);
                break;
            default:
                throw new UnsupportedOperationException("Resolution strategy " + resolutionStrategy + " is not supported.");
        }
    }

    protected void retryTask(ITaskDao.StuckTask task, AtomicInteger nResumed) {
        taskDao.markAsSubmittedAndSetNextEventTime(task.getVersionId(), getMaxStuckTime());
        BaseTask baseTask = DomainUtils.convert(task, BaseTask.class);
        baseTask.setVersion(baseTask.getVersion() + 1); // markAsSubmittedAndSetNextEventTime is bumping task version, so we will as well.
        tasksExecutionTriggerer.trigger(baseTask);
        nResumed.getAndIncrement();
        meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.stuckTasks.resumedCount", 1);
        meterHelper.registerTaskResuming(null, task.getType());
    }

    protected void markTaskAsError(IBaseTask task, AtomicInteger nError) {
        taskDao.setStatus(task.getVersionId().getId(), TaskStatus.ERROR, task.getVersionId().getVersion());
        nError.getAndIncrement();
        meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.stuckTasks.markErrorCount", 1);
        meterHelper.registerTaskMarkedAsError(null, task.getType());
    }

    public void pause() {
        paused = true;
    }

    public void resume() {
        paused = false;
    }

    private ZonedDateTime getMaxStuckTime() {
        return ZonedDateTime.now(ClockHolder.getClock()).plus(tasksProperties.getTaskStuckTimeout());
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
            List<ITaskDao.StuckTask> clientTasks = taskDao.prepareStuckOnProcessingTasksForResuming(tasksProperties.getClientId(), getMaxStuckTime());
            for (ITaskDao.StuckTask task : clientTasks) {
                if (shuttingDown) {
                    break;
                }
                MdcContext.with(() -> {
                    MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
                    log.info("Resuming client '" + tasksProperties.getClientId() + "' task " + LogUtils.asParameter(task.getVersionId()) +
                        " of type '" + task.getType() + "' in status '" + task.getStatus() + "'.");

                    tasksExecutionTriggerer.trigger(DomainUtils.convert(task, BaseTask.class));

                    meterHelper.incrementCounter(METRIC_PREFIX + "tasksResumer.clientStuckTasks.resumedCount", 1);
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
