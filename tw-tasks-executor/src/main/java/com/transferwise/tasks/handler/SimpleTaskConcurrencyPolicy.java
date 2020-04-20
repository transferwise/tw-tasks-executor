package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

public class SimpleTaskConcurrencyPolicy implements ITaskConcurrencyPolicy {

  @Getter
  @Setter
  @Accessors(chain = true)
  protected int maxConcurrency;

  protected int maxInProgressCnt;

  protected final AtomicInteger inProgressCnt = new AtomicInteger();

  public SimpleTaskConcurrencyPolicy(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
  }

  @Override
  public boolean bookSpaceForTask(IBaseTask task) {
    int cnt = inProgressCnt.incrementAndGet();
    if (cnt > maxConcurrency) {
      inProgressCnt.decrementAndGet();
      return false;
    }
    maxInProgressCnt = Math.max(maxInProgressCnt, cnt);
    return true;
  }

  @Override
  public void freeSpaceForTask(IBaseTask task) {
    if (inProgressCnt.decrementAndGet() < 0) {
      throw new IllegalStateException("Counter went below zero. Algorithm error detected.");
    }
  }
}
