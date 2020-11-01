package com.transferwise.tasks.domain;

import java.util.UUID;

public interface IBaseTask {

  UUID getId();

  long getVersion();

  ITaskVersionId getVersionId();

  @SuppressWarnings("EmptyMethod")
  String getType();

  int getPriority();
}
