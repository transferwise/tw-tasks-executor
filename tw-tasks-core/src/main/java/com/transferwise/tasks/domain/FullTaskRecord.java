package com.transferwise.tasks.domain;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class FullTaskRecord implements ITask {

  private UUID id;
  private String type;
  private String subType;
  @SuppressFBWarnings("EI_EXPOSE_REP")
  private byte[] data;
  private String status;
  private long version;
  private long processingTriesCount;
  private int priority;
  private ZonedDateTime stateTime;
  private ZonedDateTime nextEventTime;
  private String processingClientId;
  private TaskContext taskContext;

  @Override
  public ITaskVersionId getVersionId() {
    return new TaskVersionId(id, version);
  }
}
