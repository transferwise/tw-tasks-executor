package com.transferwise.tasks.helpers.executors;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorThreadFactory implements ThreadFactory {

  private final AtomicInteger threadNumber = new AtomicInteger(0);
  private final ThreadGroup group;
  private final String groupName;

  public ExecutorThreadFactory(String groupName) {
    this.groupName = groupName;

    SecurityManager s = System.getSecurityManager();
    group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();

  }

  public Thread newThread(Runnable r) {
    Thread t = new Thread(group, r, "g-" + groupName + "-t-" + threadNumber.getAndIncrement(), 0);
    if (t.isDaemon()) {
      t.setDaemon(false);
    }
    if (t.getPriority() != Thread.NORM_PRIORITY) {
      t.setPriority(Thread.NORM_PRIORITY);
    }
    return t;
  }
}
