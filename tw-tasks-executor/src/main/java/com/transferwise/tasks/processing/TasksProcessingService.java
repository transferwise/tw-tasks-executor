package com.transferwise.tasks.processing;

import com.google.common.collect.ImmutableMap;
import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.transferwise.common.baseutils.clock.ClockHolder;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.tasks.IPriorityManager;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.buckets.BucketProperties;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.helpers.executors.IExecutorsHelper;
import com.transferwise.tasks.mdc.MdcContext;
import com.transferwise.tasks.triggering.TaskTriggering;
import com.transferwise.tasks.utils.LogUtils;
import com.transferwise.tasks.utils.WaitUtils;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static com.transferwise.tasks.helpers.IMeterHelper.METRIC_PREFIX;

@Slf4j
@Transactional(propagation = Propagation.NEVER)
@SuppressWarnings("checkstyle:MultipleStringLiterals")
public class TasksProcessingService implements GracefulShutdownStrategy, ITasksProcessingService {
    @Autowired
    private ITaskDao taskDao;
    // to avoid a circular dependency when task handlers create tasks, and because we don't use this during
    // initialisation, we use lazy initialisation for this field
    @Autowired
    @Lazy
    private ITaskHandlerRegistry taskHandlerRegistry;
    @Autowired
    private TasksProperties tasksProperties;
    @Autowired
    private IExecutorsHelper executorsHelper;
    @Autowired
    private GlobalProcessingState globalProcessingState;
    @Autowired
    private IPriorityManager priorityManager;
    @Autowired
    private IBucketsManager bucketsManager;
    @Autowired
    private ITransactionsHelper transactionsHelper;
    @Autowired(required = false)
    private List<ITaskProcessingInterceptor> taskProcessingInterceptors = new ArrayList<>();
    @Autowired
    private IMeterHelper meterHelper;

    private AtomicInteger runningTasksCount = new AtomicInteger();

    private ExecutorService taskExecutor;
    private ExecutorService tasksProcessingExecutor;
    private ExecutorService tasksGrabbingExecutor;

    private volatile boolean shuttingDown;
    private AtomicInteger ongoingTasksGrabbingsCount = new AtomicInteger();

    private Consumer<TaskTriggering> taskTriggeringProcessingListener;

    private AtomicLong taskTriggeringSequence = new AtomicLong(0);

    @PostConstruct
    public void init() {
        taskExecutor = executorsHelper.newCachedExecutor("taskExecutor");
        tasksProcessingExecutor = executorsHelper.newCachedExecutor("tasksProcessing");
        tasksGrabbingExecutor = executorsHelper.newCachedExecutor("tasksGrabbing");

        meterHelper.registerGauge(METRIC_PREFIX + "processing.ongoingTasksGrabbingsCount", () -> ongoingTasksGrabbingsCount.get());
    }

    @Override
    public AddTaskForProcessingResponse addTaskForProcessing(TaskTriggering taskTriggering) {
        String bucketId = taskTriggering.getBucketId();
        BaseTask task = taskTriggering.getTask();

        String taskType = task.getType();
        if (taskType == null) {
            throw new IllegalStateException("Task " + LogUtils.asParameter(task.getVersionId()) + "' type is null.");
        }

        BucketProperties bucketProperties = bucketsManager.getBucketProperties(bucketId);

        GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);

        if (bucket.getSize().get() >= bucketProperties.getMaxTriggersInMemory()) {
            return new AddTaskForProcessingResponse().setResult(AddTaskForProcessingResponse.ResultCode.FULL);
        }

        int priority = priorityManager.normalize(task.getPriority());
        GlobalProcessingState.PrioritySlot prioritySlot = bucket.getPrioritySlot(priority);

        prioritySlot.getTaskTriggerings().add(taskTriggering);
        bucket.getSize().incrementAndGet();

        bucket.increaseVersion();

        return new AddTaskForProcessingResponse().setResult(AddTaskForProcessingResponse.ResultCode.OK);
    }

    @Override
    public void addTaskTriggeringFinishedListener(Consumer<TaskTriggering> consumer) {
        this.taskTriggeringProcessingListener = consumer;
    }

    /**
     * Finds one task triggering for which there is an available processing slot.
     * <p>
     * Return immediately as soon as one suitable task has been triggered.
     */
    protected void processTasks(GlobalProcessingState.Bucket bucket) {
        String bucketId = bucket.getBucketId();

        boolean taskProcessed = false;

        Map<String, Boolean> noRoomMap = new HashMap<>();

        for (Integer priority : bucket.getPriorities()) {
            if (taskProcessed) {
                break;
            }

            GlobalProcessingState.PrioritySlot prioritySlot = bucket.getPrioritySlot(priority);

            transferFromIntermediateBuffer(bucket, prioritySlot);

            for (GlobalProcessingState.TypeTasks typeTasks : new ArrayList<>(prioritySlot.getOrderedTypeTasks())) {
                String type = typeTasks.getType();

                if (noRoomMap.containsKey(type)) {
                    continue;
                }

                TaskTriggering taskTriggering = typeTasks.getTasks().peek();
                if (taskTriggering == null) {
                    // Empty queues are always in the end, no point of going forward.
                    break;
                }
                BaseTask task = taskTriggering.getTask();
                MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), task.getVersionId());
                try {
                    try {
                        ProcessTaskResponse processTaskResponse = grabTaskForProcessing(bucketId, task);

                        if (processTaskResponse.getResult() == ProcessTaskResponse.Result.NO_SPACE) {
                            noRoomMap.put(type, Boolean.TRUE);
                        } else {
                            // Start again, i.e. find a task with highest priority and lowest offset.
                            taskProcessed = true;
                        }
                    } catch (Throwable t) {
                        log.error("Scheduling of task '" + task.getVersionId() + "' failed.", t);
                        taskProcessed = true;
                    }

                    if (taskProcessed) {
                        taskTriggeringProcessingListener.accept(taskTriggering);
                        prioritySlot.getOrderedTypeTasks().remove(typeTasks);
                        typeTasks.getTasks().poll();
                        log.debug("Removed task '{}' triggering from processingState.", task.getVersionId());
                        prioritySlot.getOrderedTypeTasks().add(typeTasks);
                        bucket.increaseVersion();

                        break;
                    }
                } finally {
                    MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), null);
                }
            }
        }
    }

    protected void transferFromIntermediateBuffer(GlobalProcessingState.Bucket bucket, GlobalProcessingState.PrioritySlot prioritySlot) {
        while (true) {
            TaskTriggering taskTriggering = prioritySlot.getTaskTriggerings().poll();
            if (taskTriggering == null) {
                break;
            }

            taskTriggering.setSequence(taskTriggeringSequence.incrementAndGet());

            String taskType = taskTriggering.getTask().getType();
            GlobalProcessingState.TypeTasks typeTasks = prioritySlot.getTypeTasks().get(taskType);
            if (typeTasks == null) {
                prioritySlot.getTypeTasks().put(taskType, typeTasks = new GlobalProcessingState.TypeTasks().setType(taskType));
                prioritySlot.getOrderedTypeTasks().add(typeTasks);
            }
            boolean emptyQueue = typeTasks.getTasks().peek() == null;
            if (emptyQueue) {
                prioritySlot.getOrderedTypeTasks().remove(typeTasks);
            }
            typeTasks.getTasks().add(taskTriggering);
            if (emptyQueue) {
                prioritySlot.getOrderedTypeTasks().add(typeTasks);
            }

            bucket.increaseVersion();
        }
    }

    protected ProcessTaskResponse grabTaskForProcessing(String bucketId, BaseTask task) {
        GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
        BucketProperties bucketProperties = bucketsManager.getBucketProperties(bucketId);

        ITaskHandler taskHandler = taskHandlerRegistry.getTaskHandler(task);
        if (taskHandler == null) {
            boolean markAsErrorSucceeded = taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion());
            if (markAsErrorSucceeded) {
                log.error("Marking task {} as ERROR, because no task handler was found for type '{}'.",
                    LogUtils.asParameter(task.getVersionId()), task.getType());
                meterHelper.registerTaskMarkedAsError(bucketId, task.getType());
            } else {
                // Some old trigger was re-executed, logging would be spammy here.
                meterHelper.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
            }
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR);
        }

        ITaskProcessingPolicy processingPolicy = taskHandler.getProcessingPolicy(task);
        if (processingPolicy == null) {
            log.error("Marking task {} as ERROR, as the handler does not provide a processing policy.", LogUtils.asParameter(task.getVersionId()));
            if (!taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion())) {
                meterHelper.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
            } else {
                meterHelper.registerTaskMarkedAsError(bucketId, task.getType());
            }
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR);
        }

        if (!processingPolicy.canExecuteTaskOnThisNode(task)) {
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.OK);
        }

        ITaskConcurrencyPolicy concurrencyPolicy = taskHandler.getConcurrencyPolicy(task);
        if (concurrencyPolicy == null) {
            log.error("Marking task {} as ERROR, as the handler does not provide a concurrency policy.", LogUtils.asParameter(task.getVersionId()));
            if (!taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion())) {
                meterHelper.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
            } else {
                meterHelper.registerTaskMarkedAsError(bucketId, task.getType());
            }
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR);
        }

        if (!concurrencyPolicy.bookSpaceForTask(task)) {
            log.debug("There is no space to process task '{}'.", task.getVersionId());
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.NO_SPACE);
        }

        try {
            bucket.getTasksGrabbingLock().lock();
            try {
                while (bucket.getInProgressTasksGrabbingCount().incrementAndGet() > bucketProperties.getTaskGrabbingConcurrency()) {
                    bucket.getInProgressTasksGrabbingCount().decrementAndGet();
                    boolean ignored = bucket.getTasksGrabbingCondition().await(tasksProperties.getGenericMediumDelay().toMillis(), TimeUnit.MILLISECONDS);
                }
            } finally {
                bucket.getTasksGrabbingLock().unlock();
            }
            ongoingTasksGrabbingsCount.incrementAndGet();
            tasksGrabbingExecutor.submit(() -> grabTaskForProcessing0(bucket, task, concurrencyPolicy, taskHandler));
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
            concurrencyPolicy.freeSpaceForTask(task);
            if (!taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion())) {
                meterHelper.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
            }
            return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR);
        }

        return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.OK);
    }

    @Trace(dispatcher = true)
    protected void grabTaskForProcessing0(GlobalProcessingState.Bucket bucket, BaseTask task, ITaskConcurrencyPolicy concurrencyPolicy, ITaskHandler taskHandler) {
        NewRelic.setTransactionName("TwTasksEngine", "TaskGrabbing");
        try {
            ZonedDateTime maxProcessingEndTime = taskHandler.getProcessingPolicy(task).getMaxProcessingEndTime(task);

            boolean grabbed = false;
            try {
                boolean expectingVersionToBeTheSameInDb = true;
                if (tasksProperties.isCheckVersionBeforeGrabbing()) {
                    Long taskVersionInDb = taskDao.getTaskVersion(task.getId());
                    expectingVersionToBeTheSameInDb = (taskVersionInDb != null) && (taskVersionInDb == task.getVersion());
                }
                Task taskForProcessing = expectingVersionToBeTheSameInDb ? taskDao.grabForProcessing(task, tasksProperties.getClientId(), maxProcessingEndTime) : null;

                if (taskForProcessing == null) {
                    log.debug("Task '{}' was not available for processing with its version.", task.getVersionId());
                } else {
                    scheduleTask(bucket.getBucketId(), taskHandler, concurrencyPolicy, taskForProcessing);
                    grabbed = true;
                }
            } finally {
                if (!grabbed) {
                    concurrencyPolicy.freeSpaceForTask(task);
                    bucket.increaseVersion();
                    meterHelper.registerFailedTaskGrabbing(bucket.getBucketId(), task.getType());
                }
            }
        } catch (Throwable t) {
            log.error("Grabbing task '" + task.getVersionId() + "' failed.", t);
        } finally {
            bucket.getSize().decrementAndGet();
            bucket.increaseVersion();

            bucket.getTasksGrabbingLock().lock();
            try {
                bucket.getInProgressTasksGrabbingCount().decrementAndGet();
                bucket.getTasksGrabbingCondition().signalAll();
            } finally {
                bucket.getTasksGrabbingLock().unlock();
            }

            ongoingTasksGrabbingsCount.decrementAndGet();
        }
    }

    protected void scheduleTask(String bucketId, ITaskHandler taskHandler, ITaskConcurrencyPolicy concurrencyPolicy, Task taskForProcessing) {
        GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
        taskExecutor.submit(() -> {
            runningTasksCount.incrementAndGet();
            bucket.getRunningTasksCount().incrementAndGet();
            MdcContext.clear();  // Gives a clear MDC to tasks, avoids MDC leaking between tasks.
            MdcContext.put(tasksProperties.getTwTaskVersionIdMdcKey(), taskForProcessing.getVersionId());
            try {
                processTask(taskHandler, concurrencyPolicy, bucketId, taskForProcessing);
            } catch (Throwable t) {
                log.error("Processing task '" + taskForProcessing.getVersionId() + "' failed.", t);
            }
        });
    }

    protected void processWithInterceptors(int interceptorIdx, Task task, Runnable processor) {
        if (interceptorIdx >= taskProcessingInterceptors.size()) {
            processor.run();
        } else {
            taskProcessingInterceptors.get(interceptorIdx).doProcess(task, () -> processWithInterceptors(interceptorIdx + 1, task, processor));
        }
    }

    @Trace(dispatcher = true)
    protected void processTask(ITaskHandler taskHandler, ITaskConcurrencyPolicy concurrencyPolicy, String bucketId, Task task) {
        UUID taskId = task.getId();
        NewRelic.addCustomParameter(tasksProperties.getTwTaskVersionIdMdcKey(), String.valueOf(task.getVersionId()));
        NewRelic.addCustomParameter("twTaskType", task.getType());
        NewRelic.addCustomParameter("twTaskSubType", task.getSubType());
        NewRelic.setTransactionName("TwTasks", "Processing_" + task.getType());

        long processingStartTimeMs = ClockHolder.getClock().millis();
        MutableObject<ProcessingResult> processingResultHolder = new MutableObject<>(ProcessingResult.SUCCESS);

        ITaskProcessor taskProcessor = taskHandler.getProcessor(task.toBaseTask());
        processWithInterceptors(0, task, () -> {
            log.debug("Processing task '{}' using {}.", taskId, taskProcessor);
            if (taskProcessor instanceof ISyncTaskProcessor) {
                try {
                    ISyncTaskProcessor syncTaskProcessor = (ISyncTaskProcessor) taskProcessor;
                    MutableObject<ISyncTaskProcessor.ProcessResult> resultHolder = new MutableObject<>();
                    boolean isProcessorTransactional = syncTaskProcessor.isTransactional(task);
                    if (!isProcessorTransactional) {
                        meterHelper.registerTaskProcessingStart(bucketId, task.getType());
                        resultHolder.setValue(syncTaskProcessor.process(task));
                    }

                    transactionsHelper.withTransaction().asNew().call(() -> {
                        if (isProcessorTransactional) {
                            meterHelper.registerTaskProcessingStart(bucketId, task.getType());
                            resultHolder.setValue(syncTaskProcessor.process(task));
                        }
                        ISyncTaskProcessor.ProcessResult result = resultHolder.getValue();
                        if (transactionsHelper.isRollbackOnly()) {
                            log.debug("Task processor for task '{}' has crappy code. Fixing it with a rollback exception.", task.getVersionId());
                            throw new SyncProcessingRolledbackException(result);
                        }
                        if (result == null || result.getResultCode() == null || result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE) {
                            markTaskAsDone(task);
                        } else if (result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY) {
                            processingResultHolder.setValue(ProcessingResult.COMMIT_AND_RETRY);
                            setRepeatOnSuccess(bucketId, taskHandler, task);
                        } else {
                            processingResultHolder.setValue(ProcessingResult.ERROR);
                            setRetriesOrError(bucketId, taskHandler, task, null);
                        }
                        return null;
                    });
                } catch (SyncProcessingRolledbackException e) {
                    try {
                        transactionsHelper.withTransaction().asNew().call(() -> {
                            ISyncTaskProcessor.ProcessResult result = e.getProcessResult();
                            if (result == null || result.getResultCode() == null || result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE) {
                                markTaskAsDone(task);
                            } else {
                                setRetriesOrError(bucketId, taskHandler, task, null);
                            }
                            return null;
                        });
                    } catch (Throwable t) {
                        log.error("Processing task {} type: '{}' subType: '{}' failed.",
                            LogUtils.asParameter(task.getVersionId()), task.getType(), task.getSubType(), t);
                        processingResultHolder.setValue(ProcessingResult.ERROR);
                        setRetriesOrError(bucketId, taskHandler, task, t);
                    }
                } catch (Throwable t) {
                    log.error("Processing task {} type: '{}' subType: '{}' failed.",
                        LogUtils.asParameter(task.getVersionId()), task.getType(), task.getSubType(), t);
                    processingResultHolder.setValue(ProcessingResult.ERROR);
                    setRetriesOrError(bucketId, taskHandler, task, t);
                } finally {
                    taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, processingResultHolder.getValue());
                }
            }
            if (taskProcessor instanceof IAsyncTaskProcessor) {
                AtomicBoolean taskMarkedAsFinished = new AtomicBoolean(); // Buggy implementation could call our callbacks many times.
                try {
                    meterHelper.registerTaskProcessingStart(bucketId, task.getType());
                    ((IAsyncTaskProcessor) taskProcessor).process(
                        task,
                        () -> {
                            if (taskMarkedAsFinished.compareAndSet(false, true)) {
                                taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.SUCCESS);
                            }
                            markTaskAsDoneFromAsync(task);
                        },
                        (t) -> {
                            if (taskMarkedAsFinished.compareAndSet(false, true)) {
                                taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.ERROR);
                            }
                            setRetriesOrErrorFromAsync(bucketId, taskHandler, task, t);
                        });
                } catch (Throwable t) {
                    log.error("Processing task '" + task.getVersionId() + "' failed.", t);
                    if (taskMarkedAsFinished.compareAndSet(false, true)) {
                        taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.ERROR);
                    }
                    setRetriesOrError(bucketId, taskHandler, task, t);
                }
            }
        });
    }

    @Trace(dispatcher = true)
    protected void markTaskAsDoneFromAsync(Task task) {
        NewRelic.setTransactionName("TwTasksEngine", "AsyncDone");
        markTaskAsDone(task);
    }

    @Trace(dispatcher = true)
    protected void setRetriesOrErrorFromAsync(String bucketId, ITaskHandler taskHandler, Task task, Throwable t) {
        NewRelic.setTransactionName("TwTasksEngine", "AsyncRetriesOrError");
        setRetriesOrError(bucketId, taskHandler, task, t);
    }

    protected void markTaskAsDone(Task task) {
        UUID taskId = task.getId();
        log.debug("Task '{}' finished successfully.", taskId);
        if (tasksProperties.isDeleteTaskOnFinish()) {
            taskDao.deleteTask(taskId, task.getVersion());
        } else if (tasksProperties.isClearPayloadOnFinish()) {
            if (!taskDao.clearPayloadAndMarkDone(taskId, task.getVersion())) {
                meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.DONE);
            }
        } else {
            if (!taskDao.setStatus(taskId, TaskStatus.DONE, task.getVersion())) {
                meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.DONE);
            }
        }
    }

    private void setRepeatOnSuccess(String bucketId, ITaskHandler taskHandler, Task task) {
        ITaskRetryPolicy retryPolicy = taskHandler.getRetryPolicy(task.toBaseTask());
        if (retryPolicy.resetTriesCountOnSuccess(task)) {
            task.setProcessingTriesCount(0); // in DAO, it will get reset as well
        }
        ZonedDateTime retryTime = retryPolicy.getRetryTime(task, null);
        if (retryTime == null) {
            setRetriesOrError(bucketId, task, null);
        } else {
            log.debug("Repeating task '{}' will be reprocessed @ {}. Tries count reset: {}.", task.getVersionId(), retryTime, retryPolicy.resetTriesCountOnSuccess(task));
            meterHelper.registerTaskRetry(bucketId, task.getType());
            setToBeRetried(task, retryTime, retryPolicy.resetTriesCountOnSuccess(task));
        }
    }

    protected void setRetriesOrError(String bucketId, ITaskHandler taskHandler, Task task, Throwable t) {
        ZonedDateTime retryTime = taskHandler.getRetryPolicy(task.toBaseTask()).getRetryTime(task, t);
        setRetriesOrError(bucketId, task, retryTime);
    }

    protected void setRetriesOrError(String bucketId, Task task, ZonedDateTime retryTime) {
        if (retryTime == null) {
            log.info("Task {} marked as ERROR.", LogUtils.asParameter(task.getVersionId()));
            if (taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion())) {
                meterHelper.registerTaskMarkedAsError(bucketId, task.getType());
            } else {
                meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.ERROR);
            }
        } else {
            log.info("Task {} will be reprocessed @ " + retryTime + ".", LogUtils.asParameter(task.getVersionId()));
            meterHelper.registerTaskRetryOnError(bucketId, task.getType());
            setToBeRetried(task, retryTime, false);
        }
    }

    private void setToBeRetried(Task task, ZonedDateTime retryTime, boolean resetTriesCount) {
        if (!taskDao.setToBeRetried(task.getId(), retryTime, task.getVersion(), resetTriesCount)) {
            meterHelper.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.WAITING);
            log.error("Failed to setup task {} to be retried.", LogUtils.asParameter(task.getVersionId()));
        }
    }

    private void taskFinished(String bucketId, ITaskConcurrencyPolicy concurrencyPolicy, Task task, long processingStartTimeMs, ProcessingResult processingResult) {
        runningTasksCount.decrementAndGet();
        GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
        bucket.getRunningTasksCount().decrementAndGet();
        meterHelper.registerTaskProcessingEnd(bucketId, task.getType(), processingStartTimeMs, processingResult.name());

        concurrencyPolicy.freeSpaceForTask(task);
        bucket.increaseVersion();
    }

    @Override
    public void prepareForShutdown() {
        shuttingDown = true;
        tasksProcessingExecutor.shutdown();
    }

    @Override
    public boolean canShutdown() {
        return tasksProcessingExecutor.isTerminated() && runningTasksCount.get() == 0;
    }

    @Override
    public void applicationStarted() {
        for (String bucketId : bucketsManager.getBucketIds()) {
            if (!bucketsManager.getBucketProperties(bucketId).getTriggerSameTaskInAllNodes() && tasksProperties.isCheckVersionBeforeGrabbing()) {
                log.warn("Suboptimal configuration for bucket '" + bucketId + "' found. triggerSameTaskInAllNodes=false and checkVersionBeforeGrabbing=true.");
            }

            GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);

            Map<String, String> tags = ImmutableMap.of("bucketId", bucketId);
            meterHelper.registerGauge(METRIC_PREFIX + "processing.runningTasksCount", tags, () -> bucket.getRunningTasksCount().get());
            meterHelper.registerGauge(METRIC_PREFIX + "processing.inProgressTasksGrabbingCount", tags, () -> bucket.getInProgressTasksGrabbingCount().get());
            meterHelper.registerGauge(METRIC_PREFIX + "processing.triggersCount", tags, () -> bucket.getSize().get());

            tasksProcessingExecutor.submit(() -> {
                while (!shuttingDown) {
                    try {
                        long stateVersion = bucket.getVersion().get();

                        processTasks(bucket);

                        bucket.getVersionLock().lock();
                        try {
                            while (bucket.getVersion().get() == stateVersion && !shuttingDown) {
                                try {
                                    bucket.getVersionCondition().await(tasksProperties.getGenericMediumDelay().toMillis(), TimeUnit.MILLISECONDS);
                                } catch (InterruptedException e) {
                                    log.error(e.getMessage(), e);
                                }
                            }
                        } finally {
                            bucket.getVersionLock().unlock();
                        }
                    } catch (Throwable t) {
                        log.error(t.getMessage(), t);
                        WaitUtils.sleepQuietly(tasksProperties.getGenericMediumDelay());
                    }
                }
            });
        }
    }

    @Data
    @Accessors(chain = true)
    protected static class ProcessTaskResponse {
        private Result result;

        public enum Result {
            OK, NO_SPACE, ERROR
        }
    }

    protected static class SyncProcessingRolledbackException extends RuntimeException {
        @Getter
        private final ISyncTaskProcessor.ProcessResult processResult;

        public SyncProcessingRolledbackException(ISyncTaskProcessor.ProcessResult processResult) {
            super(null, null, false, false);
            this.processResult = processResult;
        }
    }

    private static enum ProcessingResult {
        SUCCESS,
        COMMIT_AND_RETRY,
        ERROR
    }
}
