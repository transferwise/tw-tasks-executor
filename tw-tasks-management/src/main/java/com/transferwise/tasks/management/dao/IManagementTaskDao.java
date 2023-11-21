package com.transferwise.tasks.management.dao;

import com.transferwise.tasks.domain.FullTaskRecord;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;

public interface IManagementTaskDao {

  @Data
  @Accessors(chain = true)
  class DaoTask1 {

    private UUID id;
    private long version;
    private String type;
    private String subType;
    private ZonedDateTime stateTime;
  }

  @Data
  @Accessors(chain = true)
  class DaoTask2 {

    private UUID id;
    private long version;
    private ZonedDateTime nextEventTime;
  }

  @Data
  @Accessors(chain = true)
  class DaoTask3 {

    private UUID id;
    private long version;
    private String type;
    private String subType;
    private String status;
    private ZonedDateTime stateTime;
    private ZonedDateTime nextEventTime;
  }

  @Data
  @Accessors(chain = true)
  class DaoTaskType {
    private String type;
    private List<String> subTypes;
  }

  List<DaoTask1> getTasksInErrorStatus(int maxCount, List<String> taskType, List<String> taskSubType);

  boolean scheduleTaskForImmediateExecution(UUID taskId, long version);

  List<DaoTask2> getStuckTasks(int maxCount, List<String> taskTypes, List<String> taskSubTypes, Duration delta);

  List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount, List<String> taskTypes, List<String> taskSubTypes);

  List<FullTaskRecord> getTasks(List<UUID> uuids);

  List<DaoTaskType> getTaskTypes();
}
