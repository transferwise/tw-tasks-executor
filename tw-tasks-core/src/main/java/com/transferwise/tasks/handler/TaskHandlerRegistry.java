package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TaskHandlerRegistry implements ITaskHandlerRegistry {

  @Autowired
  private List<ITaskHandler> handlers;

  @PostConstruct
  public void init() {
    if (log.isDebugEnabled()) {
      for (ITaskHandler taskHandler : handlers) {
        log.debug("Registering task handler: " + taskHandler);
      }
    }
  }

  @Override
  public ITaskHandler getTaskHandler(IBaseTask task) {
    List<ITaskHandler> results = handlers.stream().filter(h -> h.handles(task)).collect(Collectors.toList());

    if (results.size() > 1) {
      String resultsSt = StringUtils.join(results.stream().map(p -> p.getClass().getSimpleName()).collect(Collectors.toList()), ",");
      throw new IllegalStateException("Too many handlers are able to handle task of type '" + task.getType() + "': " + resultsSt);
    } else if (results.size() == 1) {
      return results.get(0);
    }
    return null;
  }

  @Override
  public ITaskProcessingPolicy getTaskProcessingPolicy(IBaseTask task) {
    ITaskHandler taskHandler = getTaskHandler(task);
    return taskHandler == null ? null : taskHandler.getProcessingPolicy(task);
  }
}
