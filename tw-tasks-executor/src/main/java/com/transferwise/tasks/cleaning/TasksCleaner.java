package com.transferwise.tasks.cleaning;

import com.google.common.collect.ImmutableMap;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.transferwise.common.baseutils.concurrency.ScheduledTaskExecutor;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.common.leaderselector.LeaderSelector;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.config.IExecutorServicesProvider;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.IMeterHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.transferwise.tasks.helpers.IMeterHelper.METRIC_PREFIX;

@Slf4j
@Transactional(rollbackFor = Exception.class)
public class TasksCleaner implements ITasksCleaner, GracefulShutdownStrategy {
    @Autowired
    private TasksProperties tasksProperties;
    @Autowired
    private ITaskDao taskDao;
    @Autowired
    private IExecutorServicesProvider executorServicesProvider;
    @Autowired
    private CuratorFramework curatorFramework;
    @Autowired
    private IMeterHelper meterHelper;

    private LeaderSelector leaderSelector;

    private List<DeletableStatus> deletableStatuses = new ArrayList<>();

    @PostConstruct
    @SuppressWarnings({"checkstyle:magicnumber"})
    public void init() {
        String nodePath = "/tw/tw_tasks/" + tasksProperties.getGroupId() + "/tasks_cleaner";

        TaskStatus[] statuses = {TaskStatus.DONE, TaskStatus.FAILED};
        for (TaskStatus status : statuses) {
            DeletableStatus deletableStatus = new DeletableStatus();
            deletableStatus.status = status;
            deletableStatuses.add(deletableStatus);
        }

        ExecutorService executorService = new ThreadNamingExecutorServiceWrapper("tw-tasks-cleaner", executorServicesProvider.getGlobalExecutorService());
        leaderSelector = new LeaderSelector(curatorFramework, nodePath, executorService,
            control -> {
                ScheduledTaskExecutor scheduledTaskExecutor = executorServicesProvider.getGlobalScheduledTaskExecutor();
                MutableObject<ScheduledTaskExecutor.TaskHandle> taskHandleHolder = new MutableObject<>();

                control.workAsyncUntilShouldStop(
                    () -> {
                        taskHandleHolder.setValue(scheduledTaskExecutor.scheduleAtFixedInterval(
                            this::deleteFinishedOldTasks,
                            tasksProperties.getTasksCleaningInterval(),
                            tasksProperties.getTasksCleaningInterval()));
                        log.info("Started to clean finished tasks older than " + tasksProperties.getFinishedTasksHistoryToKeep() + ".");
                    },
                    () -> {
                        log.info("Stopping tasks cleaner.");
                        if (taskHandleHolder.getValue() != null) {
                            taskHandleHolder.getValue().stop();
                            taskHandleHolder.getValue().waitUntilStopped(Duration.ofMinutes(1));
                        }
                        for (DeletableStatus deletableStatus : deletableStatuses) {
                            // We don't want old values, 0-s or -1s to be shown in Grafana when we are not even doing any work.
                            if (deletableStatus.metricHandle != null) {
                                meterHelper.unregisterMetric(deletableStatus.metricHandle);
                                deletableStatus.metricHandle = null;
                            }
                        }
                        log.info("Tasks cleaner stopped.");
                    });
            });
    }

    @Trace(dispatcher = true)
    protected void deleteFinishedOldTasks() {
        NewRelic.setTransactionName("TwTasksEngine", "OldTaskCleaning");

        for (DeletableStatus deletableStatus : deletableStatuses) {
            try {
                TaskStatus status = deletableStatus.status;
                ITaskDao.DeleteFinishedOldTasksResult result = taskDao
                    .deleteOldTasks(status, tasksProperties.getFinishedTasksHistoryToKeep(), tasksProperties.getTasksHistoryDeletingBatchSize());

                Map<String, String> tags = ImmutableMap.of("taskStatus", status.name());
                meterHelper.incrementCounter(METRIC_PREFIX + "tasksCleaner.deletableTasksCount", tags, result.getFoundTasksCount());
                meterHelper.incrementCounter(METRIC_PREFIX + "tasksCleaner.deletedTasksCount", tags, result.getDeletedTasksCount());
                meterHelper.incrementCounter(METRIC_PREFIX + "tasksCleaner.deletedUniqueKeysCount", tags, result.getDeletedUniqueKeysCount());

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
                    deletableStatus.metricHandle = meterHelper.registerGauge(METRIC_PREFIX + "tasksCleaner.deleteLagSeconds",
                        ImmutableMap.of("taskStatus", deletableStatus.status.name()), lagSecondsCopy::get);
                }

                if (log.isDebugEnabled()) {
                    log.debug("Deleted finished old tasks for status " + status.name() + ". Found: " + result.getFoundTasksCount() + ", deleted: " + result
                        .getDeletedTasksCount() + ", deleted unique keys: " + result
                        .getDeletedUniqueKeysCount() +
                        ". First task was '" + result.getFirstDeletedTaskId() + "':'" + result.getFirstDeletedTaskNextEventTime() + "', time barrier was '" + result
                        .getDeletedBeforeTime() + "'.");
                }
            } catch (Throwable t) {
                log.error(t.getMessage(), t);
            }
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

    private static class DeletableStatus {
        TaskStatus status;
        AtomicLong lagSeconds;
        Object metricHandle;
    }
}
