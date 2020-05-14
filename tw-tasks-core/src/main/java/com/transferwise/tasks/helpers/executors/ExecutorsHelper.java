package com.transferwise.tasks.helpers.executors;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ExecutorsHelper implements IExecutorsHelper {

  private static final long KEEP_ALIVE_TIMEOUT = 60L;

  @Override
  public ExecutorService newCachedExecutor(String groupName) {
    return new ThreadPoolExecutor(
        1,
        Integer.MAX_VALUE, KEEP_ALIVE_TIMEOUT,
        TimeUnit.SECONDS,
        new SynchronousQueue<>(),
        new ExecutorThreadFactory(groupName)
    );
  }

  @Override
  public ScheduledExecutorService newScheduledExecutorService(String groupName, int poolSize) {
    return new ScheduledThreadPoolExecutor(poolSize, new ExecutorThreadFactory(groupName));
  }

  @Override
  public ExecutorService newBoundedThreadPoolExecutor(String groupName, int maxThreads, int maxQueueSize, Duration maxWait) {
    return new ThreadPoolExecutor(maxThreads, maxThreads, KEEP_ALIVE_TIMEOUT, TimeUnit.SECONDS, new LinkedBlockingQueue<>(maxQueueSize),
        new ExecutorThreadFactory(groupName), (r, executor) -> {
      try {
        if (!executor.getQueue().offer(r, maxWait.toMillis(), TimeUnit.MILLISECONDS)) {
          throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
        }
      } catch (Throwable t) {
        throw new RejectedExecutionException(t);
      }
    });
  }
}
