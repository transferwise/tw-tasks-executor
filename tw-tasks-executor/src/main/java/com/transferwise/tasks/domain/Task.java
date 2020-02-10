package com.transferwise.tasks.domain;

import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Task implements ITask {

  private UUID id;
  private String type;
  private String subType;
  private String data;
  private String status;
  private long version;
  private long processingTriesCount;
  private int priority;

  // TODO: We should create an interface instead.
  public BaseTask toBaseTask() {
    return new BaseTask().setId(getId()).setPriority(getPriority()).setType(getType()).setVersion(getVersion());
  }

  @Override
  public ITaskVersionId getVersionId() {
    return new TaskVersionId(id, version);
  }
}
