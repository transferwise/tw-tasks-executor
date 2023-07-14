package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.time.Duration;
import java.util.List;
import lombok.NonNull;

public class NoOpTaskHandler implements ITaskHandler, ISyncTaskProcessor {
  private static final int MAX_PROCESSING_ATTEMPTS = 10;

  private ITaskRetryPolicy retryPolicy;
  private ITaskProcessingPolicy processingPolicy;
  private ITaskConcurrencyPolicy concurrencyPolicy;

  private List<String> handledTaskTypes;

  public NoOpTaskHandler(@NonNull List<String> handledTaskTypes) {
    this.handledTaskTypes = handledTaskTypes;
    this.retryPolicy = new ExponentialTaskRetryPolicy()
        .setMaxCount(MAX_PROCESSING_ATTEMPTS)
        .setDelay(Duration.ofSeconds(30));
    this.concurrencyPolicy = new SimpleTaskConcurrencyPolicy(3);
    this.processingPolicy =  new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(2));
  }

  @Override
  public boolean handles(IBaseTask task) {
    return handledTaskTypes.contains(task.getType());
  }

  @Override
  public ProcessResult process(ITask task) {
    return new ProcessResult().setResultCode(ResultCode.DONE);
  }

  @Override
  public ITaskProcessor getProcessor(IBaseTask task) {
    return this;
  }

  @Override
  public ITaskRetryPolicy getRetryPolicy(IBaseTask task) {
    return retryPolicy;
  }

  @Override
  public ITaskConcurrencyPolicy getConcurrencyPolicy(IBaseTask task) {
    return concurrencyPolicy;
  }

  @Override
  public ITaskProcessingPolicy getProcessingPolicy(IBaseTask task) {
    return processingPolicy;
  }

  @Override
  public boolean isTransactional(ITask task) {
    return true;
  }
}
