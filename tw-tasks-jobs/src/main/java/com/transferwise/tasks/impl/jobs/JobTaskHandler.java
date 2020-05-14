package com.transferwise.tasks.impl.jobs;

import static com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY;
import static com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode.DONE;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJobsService;
import java.time.ZonedDateTime;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class JobTaskHandler implements ITaskHandler {

  @Autowired
  protected IJobsService jobsService;

  @Autowired
  protected TasksProperties tasksProperties;

  @Autowired
  protected JobsProperties jobsProperties;

  private ITaskProcessingPolicy defaultTaskProcessingPolicy;
  private ITaskConcurrencyPolicy defaultConcurrencyPolicy;

  private ITaskRetryPolicy taskRetryPolicy;
  private ISyncTaskProcessor taskProcessor;

  @PostConstruct
  public void init() {
    defaultTaskProcessingPolicy = new SimpleTaskProcessingPolicy().setProcessingBucket(jobsProperties.getProcessingBucket());
    defaultConcurrencyPolicy = new SimpleTaskConcurrencyPolicy(jobsProperties.getConcurrency());

    taskRetryPolicy = new ITaskRetryPolicy() {
      @Override
      public ZonedDateTime getRetryTime(ITask task, Throwable t) {
        IJob job = jobsService.getJobFor(task);

        ZonedDateTime regularRunTime = job.getNextRunTime();

        if (task.getProcessingTriesCount() == 0) { // success
          return regularRunTime;
        } else { // failure
          ITaskRetryPolicy failureRetryPolicy = job.getFailureRetryPolicy();
          if (failureRetryPolicy == null) {
            return regularRunTime;
          }
          ZonedDateTime failRetryTime = failureRetryPolicy.getRetryTime(task, t);
          if (failRetryTime == null) {
            return regularRunTime;
          }
          return failRetryTime.isBefore(regularRunTime) ? failRetryTime : regularRunTime;
        }
      }

      @Override
      public boolean resetTriesCountOnSuccess(IBaseTask task) {
        return true;
      }
    };

    taskProcessor = new ISyncTaskProcessor() {
      @Override
      public ProcessResult process(ITask task) {
        IJob job = jobsService.getJobFor(task);

        IJob.ProcessResult processResult = job.process(task);
        if (processResult == null || processResult.getResultCode() == null
            || processResult.getResultCode() == IJob.ProcessResult.ResultCode.SUCCESS) {
          return new ProcessResult().setResultCode(COMMIT_AND_RETRY);
        } else {
          log.info("Stoping processing of job {} and marking it's task '{}' as DONE.", job.getUniqueName(), task.getVersionId());
          return new ProcessResult().setResultCode(DONE);
        }
      }

      @Override
      public boolean isTransactional(ITask task) {
        IJob job = jobsService.getJobFor(task);

        return job.isTransactional();
      }
    };
  }

  @Override
  public ITaskProcessor getProcessor(IBaseTask task) {
    return taskProcessor;
  }

  @Override
  public ITaskProcessingPolicy getProcessingPolicy(IBaseTask task) {
    IJob job = jobsService.getJobFor(task);

    ITaskProcessingPolicy taskProcessingPolicy = job.getProcessingPolicy();
    return taskProcessingPolicy == null ? defaultTaskProcessingPolicy : taskProcessingPolicy;
  }

  @Override
  public boolean handles(IBaseTask task) {
    return task.getType().startsWith(jobsProperties.getTaskTypePrefix())
        && jobsService.getJobFor(task) != null;
  }

  @Override
  public ITaskRetryPolicy getRetryPolicy(IBaseTask task) {
    return taskRetryPolicy;
  }

  @Override
  public ITaskConcurrencyPolicy getConcurrencyPolicy(IBaseTask task) {
    IJob cronTask = jobsService.getJobFor(task);

    ITaskConcurrencyPolicy concurrencyPolicy = cronTask.getConcurrencyPolicy();
    return concurrencyPolicy == null ? defaultConcurrencyPolicy : concurrencyPolicy;
  }
}
