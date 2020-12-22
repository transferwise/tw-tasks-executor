package com.transferwise.tasks.domain;

import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class BaseTask1 implements IBaseTask {

  private UUID id;
  private String type;
  private String key;
  private long version;
  private int priority;
  private String status;

  public BaseTask toBaseTask() {
    return new BaseTask().setVersion(getVersion()).setType(getType()).setPriority(getPriority()).setId(getId());
  }

  @Override
  public ITaskVersionId getVersionId() {
    return new TaskVersionId(id, version);
  }
}
