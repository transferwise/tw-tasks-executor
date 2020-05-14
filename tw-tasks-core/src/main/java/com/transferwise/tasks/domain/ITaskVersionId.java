package com.transferwise.tasks.domain;

import java.util.UUID;

public interface ITaskVersionId {

  UUID getId();

  long getVersion();
}
