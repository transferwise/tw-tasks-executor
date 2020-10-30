package com.transferwise.tasks.domain;

import java.time.Instant;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.MongoId;

@Accessors(chain = true)
@Data
@TypeAlias("MongoTask")
@CompoundIndex(name = "tw_task_idx1", def = "{'status': 1, 'next_event_time': 1}")
@CompoundIndex(name = "tw_task_key_index", def = "{'key':1, 'keyHash':1}")
public class MongoTask {
  public static final String ID = "_id";
  public static final String VERSION = "version";
  public static final String TYPE = "type";
  public static final String PRIORITY = "priority";
  public static final String TASK_KEY = "key";
  public static final String TASK_KEY_HASH = "keyHash";
  public static final String STATUS = "status";
  public static final String NEXT_EVENT_TIME = "nextEventTime";
  public static final String PROCESSING_CLIENT_ID = "processingClientId";
  public static final String STATE_TIME = "stateTime";
  public static final String TIME_UPDATED = "timeUpdated";
  public static final String SUB_TYPE = "subType";
  public static final String DATA = "data";
  public static final String PROCESSING_TRIES_COUNT = "processingTriesCount";
  public static final String PROCESSING_START_TIME = "processingStartTime";

  @MongoId
  private UUID id;
  private String key;
  private Integer keyHash;
  private String type;
  private String subType;
  private TaskStatus status;
  private String data;

  private Instant nextEventTime;
  private Instant stateTime;
  private Instant processingStartTime;
  private String processingClientId;
  private Integer processingTriesCount;
  private Instant timeCreated;
  private Instant timeUpdated;
  @Indexed
  private Long version;
  private Integer priority = 5;
}
