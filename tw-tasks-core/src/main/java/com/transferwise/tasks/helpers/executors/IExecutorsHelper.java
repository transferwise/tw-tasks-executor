package com.transferwise.tasks.helpers.executors;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public interface IExecutorsHelper {

  ExecutorService newCachedExecutor(String groupName);

  ScheduledExecutorService newScheduledExecutorService(String groupName, int poolSize);

  /**
   * Rejects when maxQueueSize would be exceeded.
   */
  ExecutorService newBoundedThreadPoolExecutor(String groupName, int maxThreads, int maxQueueSize, Duration maxWait);
}
