package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskExceptionHandler;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.time.ZonedDateTime;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(chain = true)
public class ChainedTaskRetryPolicy implements ITaskRetryPolicy {

  private List<ITaskRetryPolicy> retryPolicies;
  private ITaskExceptionHandler exceptionHandler;

  @Override
  public ZonedDateTime getRetryTime(ITask task, Throwable t) {
    for (ITaskRetryPolicy policy : retryPolicies) {
      ZonedDateTime retryTime = policy.getRetryTime(task, t);
      if (retryTime != null) {
        return retryTime;
      }
    }
    return null;
  }
}
