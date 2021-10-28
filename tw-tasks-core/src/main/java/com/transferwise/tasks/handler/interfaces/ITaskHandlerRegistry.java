package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;
import java.time.ZonedDateTime;
import java.util.List;

@SuppressWarnings("unused")
public interface ITaskHandlerRegistry {

  void setHandlers(List<ITaskHandler> handlers);

  ITaskHandler getTaskHandler(IBaseTask task);

  ITaskProcessingPolicy getTaskProcessingPolicy(IBaseTask task);

  ZonedDateTime getExpectedProcessingMoment(IBaseTask task);
}
