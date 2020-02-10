package com.transferwise.tasks.domain;

import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class FullTaskRecord {

  private UUID id;
  private String type;
  private String subType;
  private String data;
  private String status;
  private long version;
  private long processingTriesCount;
  private int priority;
  private ZonedDateTime stateTime;
  private ZonedDateTime nextEventTime;
  private String processingClientId;
}
