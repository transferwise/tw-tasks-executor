package com.transferwise.tasks.handler.interfaces;

import com.transferwise.tasks.domain.IBaseTask;
import java.time.ZonedDateTime;

@SuppressWarnings("unused")
public interface ITaskHandlerRegistry {

  ITaskHandler getTaskHandler(IBaseTask task);

  ITaskProcessingPolicy getTaskProcessingPolicy(IBaseTask task);

  ZonedDateTime getExpectedProcessingMoment(IBaseTask task);
}
