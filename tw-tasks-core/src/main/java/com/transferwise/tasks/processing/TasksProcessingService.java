package com.transferwise.tasks.processing;

import com.google.common.base.Preconditions;
import com.transferwise.common.baseutils.concurrency.LockUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.common.context.Criticality;
import com.transferwise.common.context.TwContext;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.context.UnitOfWork;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.tasks.IPriorityManager;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.buckets.BucketProperties;
import com.transferwise.tasks.buckets.IBucketsManager;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.entrypoints.EntryPointsNames;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.entrypoints.IMdcService;
import com.transferwise.tasks.handler.interfaces.IAsyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy.BookSpaceResponse;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
import com.transferwise.tasks.helpers.executors.IExecutorsHelper;
import com.transferwise.tasks.processing.TasksProcessingService.ProcessTaskResponse.Code;
import com.transferwise.tasks.triggering.TaskTriggering;
import com.transferwise.tasks.utils.LogUtils;
import com.transferwise.tasks.utils.WaitUtils;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.PostConstruct;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public class TasksProcessingService implements GracefulShutdownStrategy, ITasksProcessingService {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
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
  private final List<ITaskProcessingInterceptor> taskProcessingInterceptors = new ArrayList<>();
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private IMdcService mdcService;
  @Autowired
  private IEntryPointsService entryPointsHelper;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  private final AtomicInteger runningTasksCount = new AtomicInteger();

  private ExecutorService taskExecutor;
  private ExecutorService tasksProcessingExecutor;
  private ExecutorService tasksGrabbingExecutor;

  private volatile boolean shuttingDown;
  private final AtomicInteger ongoingTasksGrabbingsCount = new AtomicInteger();

  private Consumer<TaskTriggering> taskTriggeringProcessingListener;

  private final AtomicLong taskTriggeringSequence = new AtomicLong(0);

  private Instant shutdownStartTime;
  private final Set<Thread> tasksProcessingThreads = new HashSet<>();
  private final Lock tasksProcessingThreadsLock = new ReentrantLock();
  private final Lock lifecycleLock = new ReentrantLock();

  @GuardedBy("lifecycleLock")
  private boolean processingStarted;

  @PostConstruct
  public void init() {
    taskExecutor = executorsHelper.newCachedExecutor("taskExecutor");
    tasksProcessingExecutor = executorsHelper.newCachedExecutor("tasksProcessing");
    tasksGrabbingExecutor = executorsHelper.newCachedExecutor("tasksGrabbing");

    coreMetricsTemplate.registerOngoingTasksGrabbingsCount(ongoingTasksGrabbingsCount);
  }

  @Override
  public AddTaskForProcessingResponse addTaskForProcessing(TaskTriggering taskTriggering) {
    if (tasksProperties.isAssertionsEnabled()) {
      Preconditions.checkState(!TransactionSynchronizationManager.isActualTransactionActive());
    }

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
   *
   * <p>Return immediately as soon as one suitable task has been triggered.
   */
  protected ProcessTasksResponse processTasks(GlobalProcessingState.Bucket bucket) {
    String bucketId = bucket.getBucketId();
    MutableObject<Instant> tryAgainTime = new MutableObject<>();

    MutableBoolean taskProcessed = new MutableBoolean();

    Map<String, Boolean> noRoomMap = new HashMap<>();

    for (Integer priority : bucket.getPriorities()) {
      if (taskProcessed.isTrue()) {
        break;
      }
      if (tasksProperties.isDebugMetricsEnabled()) {
        coreMetricsTemplate.debugPriorityQueueCheck(bucketId, priority);
      }
      GlobalProcessingState.PrioritySlot prioritySlot = bucket.getPrioritySlot(priority);

      transferFromIntermediateBuffer(bucket, prioritySlot);

      // TODO: Probably we can avoid the new ArrayList<> here.
      for (GlobalProcessingState.TypeTasks typeTasks : new ArrayList<>(prioritySlot.getOrderedTypeTasks())) {
        if (taskProcessed.isTrue()) {
          break;
        }
        String type = typeTasks.getType();

        if (noRoomMap.containsKey(type)) {
          if (tasksProperties.isDebugMetricsEnabled()) {
            coreMetricsTemplate.debugRoomMapAlreadyHasType(bucketId, priority, type);
          }
          continue;
        }

        TaskTriggering taskTriggering = typeTasks.peek();
        if (taskTriggering == null) {
          if (tasksProperties.isDebugMetricsEnabled()) {
            coreMetricsTemplate.debugTaskTriggeringQueueEmpty(bucketId, priority, type);
          }
          // Empty queues are always in the end, no point of going forward.
          break;
        }
        BaseTask task = taskTriggering.getTask();
        // Real entry point would add too much overhead.
        mdcService.with(() -> {
          mdcService.put(task);
          try {
            ProcessTaskResponse processTaskResponse = grabTaskForProcessing(bucketId, task);

            coreMetricsTemplate.registerTaskGrabbingResponse(bucketId, type, priority, processTaskResponse);

            if (processTaskResponse.getResult() == ProcessTaskResponse.Result.NO_SPACE) {
              noRoomMap.put(type, Boolean.TRUE);
              if (processTaskResponse.getTryAgainTime() != null) {
                if (tryAgainTime.getValue() == null || tryAgainTime.getValue().isAfter(processTaskResponse.getTryAgainTime())) {
                  tryAgainTime.setValue(processTaskResponse.getTryAgainTime());
                }
              }
            } else {
              // Start again, i.e. find a task with highest priority and lowest offset.
              taskProcessed.setTrue();
            }
          } catch (Throwable t) {
            log.error("Scheduling of task '" + task.getVersionId() + "' failed.", t);
            taskProcessed.setTrue();
          }

          if (taskProcessed.isTrue()) {
            taskTriggeringProcessingListener.accept(taskTriggering);
            prioritySlot.getOrderedTypeTasks().remove(typeTasks);
            typeTasks.removeLast();
            log.debug("Removed task '{}' triggering from processingState.", task.getVersionId());
            prioritySlot.getOrderedTypeTasks().add(typeTasks);
            bucket.increaseVersion();
          }
        });
      }
    }

    ProcessTasksResponse processTasksResponse = new ProcessTasksResponse();
    if (!taskProcessed.isTrue()) {
      processTasksResponse.setTryAgainTime(tryAgainTime.getValue());
    }
    return processTasksResponse;
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

        GlobalProcessingState.TypeTasks finalTypeTasks = typeTasks;
        coreMetricsTemplate.registerProcessingTriggersCount(bucket.getBucketId(), taskType, () -> finalTypeTasks.getSize().get());

        prioritySlot.getOrderedTypeTasks().add(typeTasks);
      }
      boolean emptyQueue = typeTasks.peek() == null;
      if (emptyQueue) {
        prioritySlot.getOrderedTypeTasks().remove(typeTasks);
      }
      typeTasks.add(taskTriggering);
      if (emptyQueue) {
        prioritySlot.getOrderedTypeTasks().add(typeTasks);
      }

      bucket.increaseVersion();
    }
  }

  protected void markAsError(IBaseTask task, String bucketId) {
    boolean markAsErrorSucceeded = taskDao.setStatus(task.getVersionId().getId(), TaskStatus.ERROR, task.getVersionId().getVersion());
    if (markAsErrorSucceeded) {
      coreMetricsTemplate.registerTaskMarkedAsError(bucketId, task.getType());
    } else {
      // Some old trigger was re-executed, logging would be spammy here.
      coreMetricsTemplate.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
    }
  }

  protected ProcessTaskResponse grabTaskForProcessing(String bucketId, BaseTask task) {
    GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
    BucketProperties bucketProperties = bucketsManager.getBucketProperties(bucketId);

    ITaskHandler taskHandler = taskHandlerRegistry.getTaskHandler(task);
    if (taskHandler == null) {
      log.error("Marking task {} as ERROR, because no task handler was found for type '{}'.",
          LogUtils.asParameter(task.getVersionId()), task.getType());
      markAsError(task, bucketId);
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR).setCode(Code.NO_HANDLER);
    }

    ITaskProcessingPolicy processingPolicy = taskHandler.getProcessingPolicy(task);
    if (processingPolicy == null) {
      log.error("Marking task {} as ERROR, as the handler does not provide a processing policy.", LogUtils.asParameter(task.getVersionId()));
      markAsError(task, bucketId);
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR).setCode(Code.NO_POLICY);
    }

    if (!processingPolicy.canExecuteTaskOnThisNode(task)) {
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.OK).setCode(Code.NOT_ALLOWED_ON_NODE);
    }

    ITaskConcurrencyPolicy concurrencyPolicy = taskHandler.getConcurrencyPolicy(task);
    if (concurrencyPolicy == null) {
      log.error("Marking task {} as ERROR, as the handler does not provide a concurrency policy.", LogUtils.asParameter(task.getVersionId()));
      markAsError(task, bucketId);
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR).setCode(Code.NO_CONCURRENCY_POLICY);
    }

    BookSpaceResponse bookSpaceResponse = concurrencyPolicy.bookSpace(task);
    if (!bookSpaceResponse.isHasRoom()) {
      log.debug("There is no space to process task '{}'.", task.getVersionId());
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.NO_SPACE)
          .setTryAgainTime(bookSpaceResponse.getTryAgainTime());
    }

    try {
      bucket.getTasksGrabbingLock().lock();
      try {
        while (bucket.getInProgressTasksGrabbingCount().incrementAndGet() > bucketProperties.getTaskGrabbingMaxConcurrency()) {
          bucket.getInProgressTasksGrabbingCount().decrementAndGet();
          boolean ignored = bucket.getTasksGrabbingCondition().await(tasksProperties.getGenericMediumDelay().toMillis(), TimeUnit.MILLISECONDS);
        }
      } finally {
        bucket.getTasksGrabbingLock().unlock();
      }
      ongoingTasksGrabbingsCount.incrementAndGet();
      tasksGrabbingExecutor.submit(() -> {
        try {
          grabTaskForProcessing0(bucket, task, concurrencyPolicy, taskHandler);
        } catch (Throwable t) {
          log.error("Task grabbing failed for '{}'.", task.getVersionId(), t);
        }
      });
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
      concurrencyPolicy.freeSpace(task);
      globalProcessingState.increaseBucketsVersion();
      if (!taskDao.setStatus(task.getId(), TaskStatus.ERROR, task.getVersion())) {
        coreMetricsTemplate.registerFailedStatusChange(task.getType(), TaskStatus.UNKNOWN.name(), TaskStatus.ERROR);
      }
      return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.ERROR).setCode(Code.UNKNOWN_ERROR);
    }

    return new ProcessTaskResponse().setResult(ProcessTaskResponse.Result.OK).setCode(Code.HAPPY_FLOW);
  }

  @EntryPoint
  protected void grabTaskForProcessing0(GlobalProcessingState.Bucket bucket, BaseTask task, ITaskConcurrencyPolicy concurrencyPolicy,
      ITaskHandler taskHandler) {
    unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.GRAB_TASK).toContext()
        .execute(() -> {
          try {
            Instant maxProcessingEndTime = taskHandler.getProcessingPolicy(task).getProcessingDeadline(task);

            boolean grabbed = false;
            try {
              boolean expectingVersionToBeTheSameInDb = true;
              if (tasksProperties.isCheckVersionBeforeGrabbing()) {
                Long taskVersionInDb = taskDao.getTaskVersion(task.getId());
                expectingVersionToBeTheSameInDb = (taskVersionInDb != null) && (taskVersionInDb == task.getVersion());
              }
              Task taskForProcessing =
                  expectingVersionToBeTheSameInDb ? taskDao.grabForProcessing(task, tasksProperties.getClientId(), maxProcessingEndTime) : null;

              if (taskForProcessing == null) {
                log.debug("Task '{}' was not available for processing with its version.", task.getVersionId());
              } else {
                scheduleTask(bucket.getBucketId(), taskHandler, concurrencyPolicy, taskForProcessing);
                grabbed = true;
              }
            } finally {
              if (!grabbed) {
                concurrencyPolicy.freeSpace(task);
                globalProcessingState.increaseBucketsVersion();
                coreMetricsTemplate.registerFailedTaskGrabbing(bucket.getBucketId(), task.getType());
              }
            }
          } catch (Throwable t) {
            log.error("Grabbing task '" + task.getVersionId() + "' failed.", t);
          } finally {
            bucket.getSize().decrementAndGet();
            bucket.increaseVersion();

            Lock tasksGrabbingLock = bucket.getTasksGrabbingLock();
            tasksGrabbingLock.lock();
            try {
              bucket.getInProgressTasksGrabbingCount().decrementAndGet();
              bucket.getTasksGrabbingCondition().signalAll();
            } finally {
              tasksGrabbingLock.unlock();
            }

            ongoingTasksGrabbingsCount.decrementAndGet();
          }
        });
  }

  protected void scheduleTask(String bucketId, ITaskHandler taskHandler, ITaskConcurrencyPolicy concurrencyPolicy, Task task) {
    taskExecutor.submit(() -> {
      try {
        processTask(taskHandler, concurrencyPolicy, bucketId, task);
      } catch (Throwable t) {
        log.error("Task processing failed for '{}'.", task.getVersionId(), t);
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

  @EntryPoint
  protected void processTask(ITaskHandler taskHandler, ITaskConcurrencyPolicy concurrencyPolicy, String bucketId, Task task) {
    mdcService.clear();  // Gives a clear MDC to tasks, avoids MDC leaking between tasks.
    try {
      unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS, EntryPointsNames.PROCESSING + task.getType()).toContext()
          .execute(() -> {
            runningTasksCount.incrementAndGet();

            GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
            bucket.getRunningTasksCount().incrementAndGet();

            mdcService.put(task);

            final UUID taskId = task.getId();

            ITaskProcessingPolicy processingPolicy = taskHandler.getProcessingPolicy(task);
            Instant deadline = processingPolicy.getProcessingDeadline(task);
            Criticality criticality = processingPolicy.getProcessingCriticality(task);
            String owner = processingPolicy.getOwner(task);

            TwContext currentContext = TwContext.current();
            UnitOfWork unitOfWork = unitOfWorkManager.getUnitOfWork(currentContext);
            unitOfWork.setDeadline(deadline);
            unitOfWork.setCriticality(criticality);
            if (owner != null) {
              currentContext.setOwner(owner);
            }

            long processingStartTimeMs = TwContextClockHolder.getClock().millis();
            MutableObject<ProcessingResult> processingResultHolder = new MutableObject<>(ProcessingResult.SUCCESS);

            ITaskProcessor taskProcessor = taskHandler.getProcessor(task.toBaseTask());
            processWithInterceptors(0, task, () -> {
              log.debug("Processing task '{}' using {}.", taskId, taskProcessor);
              if (taskProcessor instanceof ISyncTaskProcessor) {
                try {
                  ISyncTaskProcessor syncTaskProcessor = (ISyncTaskProcessor) taskProcessor;
                  MutableObject<ISyncTaskProcessor.ProcessResult> resultHolder = new MutableObject<>();
                  boolean isProcessorTransactional = syncTaskProcessor.isTransactional(task);

                  Runnable process = () -> {
                    coreMetricsTemplate.registerTaskProcessingStart(bucketId, task.getType());
                    LockUtils.withLock(tasksProcessingThreadsLock, () -> tasksProcessingThreads.add(Thread.currentThread()));
                    try {
                      resultHolder.setValue(syncTaskProcessor.process(task));
                    } finally {
                      LockUtils.withLock(tasksProcessingThreadsLock, () -> tasksProcessingThreads.remove(Thread.currentThread()));
                    }
                  };

                  if (!isProcessorTransactional) {
                    process.run();
                  }

                  transactionsHelper.withTransaction().asNew().call(() -> {
                    if (isProcessorTransactional) {
                      process.run();
                    }
                    ISyncTaskProcessor.ProcessResult result = resultHolder.getValue();
                    if (transactionsHelper.isRollbackOnly()) {
                      log.debug("Task processor for task '{}' has crappy code. Fixing it with a rollback exception.", task.getVersionId());
                      throw new SyncProcessingRolledbackException(result);
                    }
                    if (result == null || result.getResultCode() == null
                        || result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE) {
                      markTaskAsDone(task);
                    } else if (result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY) {
                      processingResultHolder.setValue(ProcessingResult.COMMIT_AND_RETRY);
                      setRepeatOnSuccess(bucketId, taskHandler, task);
                    } else if (result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE_AND_DELETE) {
                      deleteTask(task);
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
                      if (result == null || result.getResultCode() == null
                          || result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE) {
                        markTaskAsDone(task);
                      } else if (result.getResultCode() == ISyncTaskProcessor.ProcessResult.ResultCode.DONE_AND_DELETE) {
                        deleteTask(task);
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
                  // Clear the interrupted flag.
                  if (Thread.interrupted()) {
                    log.debug("Task {} got interrupted.", LogUtils.asParameter(task.getVersionId()));
                  }
                  log.error("Processing task {} type: '{}' subType: '{}' failed.",
                      LogUtils.asParameter(task.getVersionId()), task.getType(), task.getSubType(), t);
                  processingResultHolder.setValue(ProcessingResult.ERROR);
                  setRetriesOrError(bucketId, taskHandler, task, t);
                } finally {
                  taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, processingResultHolder.getValue());
                }
              } else if (taskProcessor instanceof IAsyncTaskProcessor) {
                AtomicBoolean taskMarkedAsFinished = new AtomicBoolean(); // Buggy implementation could call our callbacks many times.
                try {
                  coreMetricsTemplate.registerTaskProcessingStart(bucketId, task.getType());
                  ((IAsyncTaskProcessor) taskProcessor).process(task,
                      () -> markTaskAsDoneFromAsync(task, taskMarkedAsFinished.compareAndSet(false, true), bucketId, concurrencyPolicy,
                          processingStartTimeMs),
                      (t) -> setRetriesOrErrorFromAsync(task, taskMarkedAsFinished.compareAndSet(false, true), bucketId, concurrencyPolicy,
                          processingStartTimeMs, taskHandler, t));
                } catch (Throwable t) {
                  log.error("Processing task '" + task.getVersionId() + "' failed.", t);
                  if (taskMarkedAsFinished.compareAndSet(false, true)) {
                    taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.ERROR);
                  }
                  setRetriesOrError(bucketId, taskHandler, task, t);
                }
              } else {
                log.error(
                    "Marking task {} as ERROR, because No suitable processor found for task " + LogUtils.asParameter(task.getVersionId())
                        + ".");
                markAsError(task, bucketId);
              }
            });
          });
    } catch (Throwable t) {
      log.error("Processing task '" + task.getVersionId() + "' failed.", t);
    } finally {
      mdcService.clear();
    }
  }

  @EntryPoint(usesExisting = true)
  protected void markTaskAsDoneFromAsync(Task task, boolean markAsFinished, String bucketId, ITaskConcurrencyPolicy concurrencyPolicy,
      long processingStartTimeMs) {
    entryPointsHelper.continueOrCreate(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.ASYNC_HANDLE_SUCCESS,
        () -> {
          mdcService.put(task);
          if (markAsFinished) {
            taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.SUCCESS);
          }

          markTaskAsDone(task);
          return null;
        });
  }

  @EntryPoint(usesExisting = true)
  protected void setRetriesOrErrorFromAsync(Task task, boolean markAsFinished, String bucketId, ITaskConcurrencyPolicy concurrencyPolicy,
      long processingStartTimeMs, ITaskHandler taskHandler, Throwable t) {
    entryPointsHelper.continueOrCreate(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.ASYNC_HANDLE_FAIL,
        () -> {
          mdcService.put(task);
          if (markAsFinished) {
            taskFinished(bucketId, concurrencyPolicy, task, processingStartTimeMs, ProcessingResult.ERROR);
          }

          setRetriesOrError(bucketId, taskHandler, task, t);
          return null;
        });
  }

  protected void markTaskAsDone(Task task) {
    UUID taskId = task.getId();
    log.debug("Task '{}' finished successfully.", taskId);
    if (tasksProperties.isDeleteTaskOnFinish()) {
      taskDao.deleteTask(taskId, task.getVersion());
    } else if (tasksProperties.isClearPayloadOnFinish()) {
      if (!taskDao.clearPayloadAndMarkDone(taskId, task.getVersion())) {
        coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.DONE);
      }
    } else {
      if (!taskDao.setStatus(taskId, TaskStatus.DONE, task.getVersion())) {
        coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.DONE);
      }
    }
  }

  private void deleteTask(Task task) {
    UUID taskId = task.getId();
    log.debug("Task '{}' finished successfully, deleting.", taskId);
    taskDao.deleteTask(taskId, task.getVersion());
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
      log.debug("Repeating task '{}' will be reprocessed @ {}. Tries count reset: {}.", task.getVersionId(), retryTime,
          retryPolicy.resetTriesCountOnSuccess(task));
      coreMetricsTemplate.registerTaskRetry(bucketId, task.getType());
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
        coreMetricsTemplate.registerTaskMarkedAsError(bucketId, task.getType());
      } else {
        coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.ERROR);
      }
    } else {
      log.info("Task {} will be reprocessed @ " + retryTime + ".", LogUtils.asParameter(task.getVersionId()));
      coreMetricsTemplate.registerTaskRetryOnError(bucketId, task.getType());
      setToBeRetried(task, retryTime, false);
    }
  }

  private void setToBeRetried(Task task, ZonedDateTime retryTime, boolean resetTriesCount) {
    if (!taskDao.setToBeRetried(task.getId(), retryTime, task.getVersion(), resetTriesCount)) {
      coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.WAITING);
    }
  }

  private void taskFinished(String bucketId, ITaskConcurrencyPolicy concurrencyPolicy, Task task, long processingStartTimeMs,
      ProcessingResult processingResult) {
    runningTasksCount.decrementAndGet();
    GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);
    bucket.getRunningTasksCount().decrementAndGet();
    coreMetricsTemplate.registerTaskProcessingEnd(bucketId, task.getType(), processingStartTimeMs, processingResult.name());

    concurrencyPolicy.freeSpace(task);
    globalProcessingState.increaseBucketsVersion();
  }

  @Override
  public void prepareForShutdown() {
    shutdownStartTime = Instant.now(TwContextClockHolder.getClock());
    shuttingDown = true;
    tasksProcessingExecutor.shutdown();
  }

  @Override
  public boolean canShutdown() {
    if (tasksProperties.getInterruptTasksAfterShutdownTime() != null
        && Duration.between(shutdownStartTime, Instant.now(TwContextClockHolder.getClock()))
        .compareTo(tasksProperties.getInterruptTasksAfterShutdownTime()) > 0) {
      LockUtils.withLock(tasksProcessingThreadsLock, () -> {
        log.info("taskProcessingThreads: " + tasksProcessingThreads.size());
        for (Thread thread : tasksProcessingThreads) {
          log.info("Interrupting thread: " + thread);
          thread.interrupt();
        }
      });
    }
    return tasksProcessingExecutor.isTerminated() && runningTasksCount.get() == 0;
  }

  @Override
  public void startProcessing() {
    lifecycleLock.lock();
    try {
      if (processingStarted) {
        return;
      }
      for (String bucketId : bucketsManager.getBucketIds()) {
        if (!bucketsManager.getBucketProperties(bucketId).getTriggerSameTaskInAllNodes() && tasksProperties.isCheckVersionBeforeGrabbing()) {
          log.warn(
              "Suboptimal configuration for bucket '" + bucketId + "' found. triggerSameTaskInAllNodes=false and checkVersionBeforeGrabbing=true.");
        }

        GlobalProcessingState.Bucket bucket = globalProcessingState.getBuckets().get(bucketId);

        coreMetricsTemplate.registerRunningTasksCount(bucketId, () -> bucket.getRunningTasksCount().get());
        coreMetricsTemplate.registerInProgressTasksGrabbingCount(bucketId, () -> bucket.getInProgressTasksGrabbingCount().get());
        coreMetricsTemplate.registerProcessingTriggersCount(bucketId, () -> bucket.getSize().get());
        coreMetricsTemplate.registerProcessingStateVersion(bucketId, () -> bucket.getVersion().get());

        tasksProcessingExecutor.submit(() -> {
          while (!shuttingDown) {
            try {
              long stateVersion = bucket.getVersion().get();

              ProcessTasksResponse processTasksResponse = processTasks(bucket);
              Instant tryAgainTime = processTasksResponse.getTryAgainTime();

              Lock versionLock = bucket.getVersionLock();
              versionLock.lock();
              try {
                if (bucket.getVersion().get() == stateVersion && !shuttingDown) {
                  long defaultWaitTimeMs = tasksProperties.getGenericMediumDelay().toMillis();
                  long tryAgainWaitTimeMs =
                      tryAgainTime == null ? Integer.MAX_VALUE : tryAgainTime.toEpochMilli() - TwContextClockHolder.getClock().millis();
                  long waitTimeMs = Math.min(defaultWaitTimeMs, tryAgainWaitTimeMs);

                  if (waitTimeMs > 0) {
                    try {
                      bucket.getVersionCondition().await(waitTimeMs, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                      log.error(e.getMessage(), e);
                    }
                  }
                }
              } finally {
                versionLock.unlock();
              }
            } catch (Throwable t) {
              log.error(t.getMessage(), t);
              WaitUtils.sleepQuietly(tasksProperties.getGenericMediumDelay());
            }
          }
        });
      }
      processingStarted = true;
    } finally {
      lifecycleLock.unlock();
    }
  }

  @lombok.Data
  @lombok.experimental.Accessors(chain = true)
  public static class ProcessTaskResponse {

    private Result result;

    private Code code;

    private Instant tryAgainTime;

    public enum Result {
      OK, NO_SPACE, ERROR
    }

    public enum Code {
      NO_HANDLER, NO_POLICY, NO_CONCURRENCY_POLICY, UNKNOWN_ERROR, HAPPY_FLOW, NOT_ALLOWED_ON_NODE
    }
  }

  protected static class SyncProcessingRolledbackException extends RuntimeException {

    static final long serialVersionUID = 1L;

    @Getter
    private final ISyncTaskProcessor.ProcessResult processResult;

    public SyncProcessingRolledbackException(ISyncTaskProcessor.ProcessResult processResult) {
      super(null, null, false, false);
      this.processResult = processResult;
    }
  }

  private enum ProcessingResult {
    SUCCESS,
    COMMIT_AND_RETRY,
    ERROR
  }

  @Data
  @Accessors(chain = true)
  private static class ProcessTasksResponse {

    private Instant tryAgainTime;
  }
}
