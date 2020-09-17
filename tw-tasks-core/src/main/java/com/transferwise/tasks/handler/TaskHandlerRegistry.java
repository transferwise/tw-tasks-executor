package com.transferwise.tasks.handler;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class TaskHandlerRegistry implements ITaskHandlerRegistry {

  @Autowired
  private ApplicationContext applicationContext;

  @Autowired
  private TasksProperties tasksProperties;

  private volatile List<ITaskHandler> handlers;

  @Override
  public ITaskHandler getTaskHandler(IBaseTask task) {
    // TODO: Should we add caching here for larger applications?
    // TODO: Or at least advice about keeping the number of handlers low in docs.
    // TODO: Applications themselves can create hierarchical handlers themselves being able to handle most types of tasks. 
    ITaskHandler result = null;
    for (ITaskHandler taskHandler : getHandlers()) {
      if (taskHandler.handles(task)) {
        if (result != null) {
          String handlersSt = getHandlers().stream().filter(h -> h.handles(task)).map(h -> h.getClass().getSimpleName())
              .collect(Collectors.joining(","));
          throw new IllegalStateException("Too many handlers are able to handle task of type '" + task.getType() + "': " + handlersSt);
        } else {
          result = taskHandler;
        }
      }
    }
    return result;
  }

  @Override
  public ITaskProcessingPolicy getTaskProcessingPolicy(IBaseTask task) {
    ITaskHandler taskHandler = getTaskHandler(task);
    return taskHandler == null ? null : taskHandler.getProcessingPolicy(task);
  }

  @Override
  public ZonedDateTime getExpectedProcessingMoment(IBaseTask task) {
    ITaskProcessingPolicy taskProcessingPolicy = getTaskProcessingPolicy(task);
    Duration timeout = taskProcessingPolicy != null ? taskProcessingPolicy.getExpectedQueueTime(task) : null;
    if (timeout == null) {
      timeout = tasksProperties.getTaskStuckTimeout();
    }
    return ZonedDateTime.now(TwContextClockHolder.getClock()).plus(timeout);
  }

  /**
   * Task handlers should be lazily loaded to avoid any circular dependencies.
   */
  protected List<ITaskHandler> getHandlers() {
    if (handlers == null) {
      synchronized (this) {
        if (handlers == null) {
          handlers = new ArrayList<>(applicationContext.getBeansOfType(ITaskHandler.class).values());
          if (log.isDebugEnabled()) {
            for (ITaskHandler taskHandler : handlers) {
              log.debug("Registering task handler: " + taskHandler);
            }
          }
        }
      }
    }
    return handlers;
  }
}
