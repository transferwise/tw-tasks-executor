package com.transferwise.tasks;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.entrypoints.EntryPointsGroups;
import com.transferwise.tasks.entrypoints.EntryPointsNames;
import com.transferwise.tasks.entrypoints.IEntryPointsService;
import com.transferwise.tasks.entrypoints.IMdcService;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.helpers.ICoreMetricsTemplate;
import com.transferwise.tasks.helpers.executors.IExecutorsHelper;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import com.transferwise.tasks.utils.LogUtils;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Slf4j
public class TasksService implements ITasksService, GracefulShutdownStrategy {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private ITasksExecutionTriggerer tasksExecutionTriggerer;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private IExecutorsHelper executorsHelper;
  @Autowired
  private IPriorityManager priorityManager;
  @Autowired
  private ITaskHandlerRegistry taskHandlerRegistry;
  @Autowired
  private IMdcService mdcService;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;
  @Autowired
  private IEntryPointsService entryPointsHelper;
  @Autowired
  private IEnvironmentValidator environmentValidator;
  @Autowired
  private ICoreMetricsTemplate coreMetricsTemplate;

  private ExecutorService afterCommitExecutorService;
  private TxSyncAdapterFactory txSyncAdapterFactory;

  private final AtomicInteger inProgressAfterCommitTasks = new AtomicInteger();
  private final AtomicInteger activeAfterCommitTasks = new AtomicInteger();

  @PostConstruct
  public void init() {
    environmentValidator.validate();

    if (tasksProperties.isAsyncTaskTriggering()) {
      afterCommitExecutorService = executorsHelper.newScheduledExecutorService("tsac", tasksProperties.getAsyncTaskTriggeringsConcurrency());
      txSyncAdapterFactory = AsynchronouslyTriggerTaskTxSyncAdapter::new;
    } else {
      txSyncAdapterFactory = (mdcService, unitOfWorkManager, task) -> new SynchronouslyTriggerTaskTxSyncAdapter(task);
    }

    log.info("Tasks service initialized for client '" + tasksProperties.getClientId() + "'.");

    coreMetricsTemplate.registerInProgressTriggeringsCount(inProgressAfterCommitTasks);
    coreMetricsTemplate.registerActiveTriggeringsCount(activeAfterCommitTasks);
  }

  @Override
  @EntryPoint(usesExisting = true)
  @Transactional(rollbackFor = Exception.class)
  public AddTaskResponse addTask(AddTaskRequest request) {
    return entryPointsHelper.continueOrCreate(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.ADD_TASK,
        () -> {
          mdcService.put(request.getTaskId(), 0L);
          mdcService.putType(request.getType());
          mdcService.putSubType(request.getSubType());
          ZonedDateTime now = ZonedDateTime.now(TwContextClockHolder.getClock());
          final TaskStatus status =
              request.getRunAfterTime() == null || !request.getRunAfterTime().isAfter(now) ? TaskStatus.SUBMITTED : TaskStatus.WAITING;

          final int priority = priorityManager.normalize(request.getPriority());
          if (StringUtils.isEmpty(StringUtils.trim(request.getType()))) {
            throw new IllegalStateException("Task type is mandatory, but '" + request.getType() + "' was provided.");
          }

          ZonedDateTime maxStuckTime =
              request.getExpectedQueueTime() == null ? now.plus(tasksProperties.getTaskStuckTimeout()) : now.plus(request.getExpectedQueueTime());
          byte[] data = request.getData();
          ITaskDao.InsertTaskResponse insertTaskResponse = taskDao.insertTask(
              new ITaskDao.InsertTaskRequest().setData(data).setKey(request.getUniqueKey())
                  .setRunAfterTime(request.getRunAfterTime())
                  .setSubType(request.getSubType())
                  .setType(request.getType()).setTaskId(request.getTaskId())
                  .setMaxStuckTime(maxStuckTime).setStatus(status).setPriority(priority)
                  .setCompression(request.getCompression()));

          coreMetricsTemplate
              .registerTaskAdding(request.getType(), request.getUniqueKey(), insertTaskResponse.isInserted(), request.getRunAfterTime(), data);

          if (!insertTaskResponse.isInserted()) {
            coreMetricsTemplate.registerDuplicateTask(request.getType(), !request.isWarnWhenTaskExists());
            if (request.isWarnWhenTaskExists()) {
              log.warn("Task with uuid '" + request.getTaskId() + "'"
                  + (request.getUniqueKey() == null ? "" : " and key '" + request.getUniqueKey() + "'")
                  + " already exists (type " + request.getType() + ", subType " + request.getSubType() + ").");
            }
            return new AddTaskResponse().setResult(AddTaskResponse.Result.ALREADY_EXISTS);
          }

          final UUID taskId = insertTaskResponse.getTaskId();
          mdcService.put(taskId, 0L);
          log.debug("Task '{}' created with status {}.", taskId, status);
          if (status == TaskStatus.SUBMITTED) {
            triggerTask(new BaseTask().setId(taskId).setType(request.getType()).setPriority(priority));
          }

          return new AddTaskResponse().setResult(AddTaskResponse.Result.OK).setTaskId(taskId);
        });
  }

  @Override
  @EntryPoint(usesExisting = true)
  @Transactional(rollbackFor = Exception.class)
  public boolean resumeTask(ResumeTaskRequest request) {
    return entryPointsHelper.continueOrCreate(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.RESUME_TASK,
        () -> {
          UUID taskId = request.getTaskId();
          mdcService.put(request.getTaskId(), request.getVersion());

          BaseTask1 task = taskDao.getTask(taskId, BaseTask1.class);

          if (task == null) {
            log.debug("Cannot resume task '" + taskId + "' as it was not found.");
            return false;
          }

          mdcService.put(task);

          long version = task.getVersion();

          if (version != request.getVersion()) {
            coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
            log.debug("Expected version " + request.getVersion() + " does not match " + version + ".");
            return false;
          }

          if (task.getStatus().equals(TaskStatus.WAITING.name()) || task.getStatus().equals(TaskStatus.NEW.name())) {
            if (!taskDao.markAsSubmitted(taskId, version++, taskHandlerRegistry.getExpectedProcessingMoment(task))) {
              coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
              if (log.isDebugEnabled()) {
                log.debug("Can not resume task '" + taskId + "', expected version " + request.getVersion()
                    + " does not match " + task.getVersion() + ".");
              }
              return false;
            }
          } else if (!task.getStatus().equals(TaskStatus.SUBMITTED.name())) {
            if (!request.isForce()) {
              if (log.isDebugEnabled()) {
                log.debug("Can not resume task {}, it has wrong state '{}'.", LogUtils.asParameter(task.getVersionId()), task.getStatus());
              }
              return false;
            } else {
              log.warn("Task '" + taskId + "' will be force resumed. Status will change from '" + task.getStatus() + "' to 'SUBMITTED'.");
            }
            if (!taskDao.markAsSubmitted(taskId, version++, taskHandlerRegistry.getExpectedProcessingMoment(task))) {
              coreMetricsTemplate.registerFailedStatusChange(task.getType(), task.getStatus(), TaskStatus.SUBMITTED);
              log.debug("Can not resume task {}, it has wrong version '{}'.", LogUtils.asParameter(task.getVersionId()), task.getStatus());
              return false;
            }
          }
          triggerTask(task.toBaseTask().setVersion(version));
          return true;
        });
  }

  @Override
  public void startTasksProcessing(String bucketId) {
    tasksExecutionTriggerer.startTasksProcessing(bucketId);
  }

  @Override
  public Future<Void> stopTasksProcessing(String bucketId) {
    return tasksExecutionTriggerer.stopTasksProcessing(bucketId);
  }

  @Override
  public ITasksService.TasksProcessingState getTasksProcessingState(String bucketId) {
    return tasksExecutionTriggerer.getTasksProcessingState(bucketId);
  }

  /**
   * We register an after commit hook here, so as soon as transaction has finished, the task will be triggered.
   *
   * <p>The problem with many Spring provided transactions manager, is that this hook is called, after commit has happened,
   * but before the resources (db connections) are released.
   *
   * <p>If we would do it in the same thread, and because triggering needs its own database connection for marking as submit,
   * we would create a deadlock on pool exhaustion situations (easiest to test with db pool of 1).
   *
   * <p>So we do it asynchronously in different threads. In this case, if triggering will be slower than creating tasks,
   * we may exhaust memory, so a maximum limit of waiting triggerings has to be enforced.
   *
   * <p>Notice, that with proper JTA transaction manager (TW uses Gaffer in some services),
   * this deadlock problem does not exist and we may trigger in the same thread.
   */
  protected void triggerTask(BaseTask task) {
    TransactionSynchronizationManager.registerSynchronization(txSyncAdapterFactory.create(mdcService, unitOfWorkManager, task));
  }

  @Override
  public boolean canShutdown() {
    return inProgressAfterCommitTasks.get() == 0;
  }

  private void doTriggerTask(BaseTask task) {
    activeAfterCommitTasks.incrementAndGet();
    try {
      // TODO: If trigger would not need a database connection or at least no new transaction, deadlock situation could be prevented.
      // TODO: Do we want to mark NEW tasks immediately as SUBMITTED and then even remove the separate SUBMITTED state?
      // TODO: We probably still need SUBMITTED to separate NEW and WAITING.
      tasksExecutionTriggerer.trigger(task);
      if (log.isDebugEnabled()) {
        log.debug("Task {} triggered. AfterCommit queue size is {}.", LogUtils.asParameter(task.getVersionId()), inProgressAfterCommitTasks.get());
      }
    } catch (Throwable t) {
      log.error("Triggering task '{}' failed.", task.getVersionId(), t);
    } finally {
      activeAfterCommitTasks.decrementAndGet();
    }
  }

  @FunctionalInterface
  private interface TxSyncAdapterFactory {

    TransactionSynchronizationAdapter create(IMdcService mdcService, UnitOfWorkManager unitOfWorkManager, BaseTask task);
  }

  @RequiredArgsConstructor
  private class SynchronouslyTriggerTaskTxSyncAdapter extends TransactionSynchronizationAdapter {

    private final BaseTask task;

    @Override
    public void afterCommit() {
      inProgressAfterCommitTasks.incrementAndGet();
      try {
        doTriggerTask(task);
      } finally {
        inProgressAfterCommitTasks.decrementAndGet();
      }
    }
  }

  @RequiredArgsConstructor
  private class AsynchronouslyTriggerTaskTxSyncAdapter extends TransactionSynchronizationAdapter {

    private final IMdcService mdcService;
    private final UnitOfWorkManager unitOfWorkManager;
    private final BaseTask task;

    @Override
    public void afterCommit() {
      if (inProgressAfterCommitTasks.incrementAndGet() >= tasksProperties.getMaxAsyncTaskTriggerings()) {
        // Need to ignore, if we wait for empty space, we can create deadlock with database resources.
        log.warn("Task {} was not triggered, because resources have been exhausted.", LogUtils.asParameter(task.getVersionId()));
        inProgressAfterCommitTasks.decrementAndGet();
        return;
      }

      afterCommitExecutorService.submit(() -> {
        try {
          afterCommit0();
        } catch (Throwable t) {
          log.error("Triggering task '{}' failed.", task.getVersionId(), t);
        }
      });
    }

    @EntryPoint
    private void afterCommit0() {
      unitOfWorkManager.createEntryPoint(EntryPointsGroups.TW_TASKS_ENGINE, EntryPointsNames.TRIGGER_TASK).toContext().execute(
          () -> {
            try {
              mdcService.put(task);
              doTriggerTask(task);
            } finally {
              inProgressAfterCommitTasks.decrementAndGet();
            }
          });

    }
  }

}
