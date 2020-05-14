package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import java.time.ZonedDateTime;

public class NoRetryPolicy implements ITaskRetryPolicy {

  @Override
  public ZonedDateTime getRetryTime(ITask task, Throwable t) {
    return null;
  }
}
