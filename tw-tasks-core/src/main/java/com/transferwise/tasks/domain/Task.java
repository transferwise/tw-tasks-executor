package com.transferwise.tasks.domain;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Task implements ITask {

  private UUID id;
  private String type;
  private String subType;
  @SuppressFBWarnings("EI_EXPOSE_REP")
  private byte[] data;
  private String status;
  private long version;
  private long processingTriesCount;
  private int priority;
  private TaskContext taskContext;

  // TODO: We should create an interface instead.
  public BaseTask toBaseTask() {
    return new BaseTask().setId(getId()).setPriority(getPriority()).setType(getType()).setVersion(getVersion());
  }

  @Override
  public ITaskVersionId getVersionId() {
    return new TaskVersionId(id, version);
  }
}
