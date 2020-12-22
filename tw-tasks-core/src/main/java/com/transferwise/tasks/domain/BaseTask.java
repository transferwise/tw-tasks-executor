package com.transferwise.tasks.domain;

import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class BaseTask implements IBaseTask {

  private UUID id;
  private String type;
  private String key;
  private long version;
  private int priority;

  public ITaskVersionId getVersionId() {
    return new TaskVersionId(id, version);
  }
}
