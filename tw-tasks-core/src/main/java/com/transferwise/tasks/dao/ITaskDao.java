package com.transferwise.tasks.dao;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;

public interface ITaskDao {

  @Data
  @Accessors(chain = true)
  class StuckTask implements IBaseTask {

    private TaskVersionId versionId;
    private int priority;
    private String type;
    private String status;
  }

  @Data
  @Accessors(chain = true)
  class InsertTaskRequest {

    private String type;
    private String subType;
    private String data;
    private UUID taskId;
    private String key;
    private ZonedDateTime runAfterTime;
    private TaskStatus status;
    private ZonedDateTime maxStuckTime;
    private Integer priority;
  }

  @Data
  @Accessors(chain = true)
  class InsertTaskResponse {

    private UUID taskId;
    private boolean inserted;
  }

  @Data
  @Accessors(chain = true)
  class GetStuckTasksResponse {

    private List<StuckTask> stuckTasks;
    private boolean hasMore;
  }

  @Data
  @Accessors(chain = true)
  class DeleteFinishedOldTasksResult {

    private int foundTasksCount;
    private int deletedTasksCount;
    private int deletedUniqueKeysCount;
    private UUID firstDeletedTaskId;
    private ZonedDateTime firstDeletedTaskNextEventTime;
    private ZonedDateTime deletedBeforeTime;
  }

  ZonedDateTime getEarliestTaskNextEventTime(TaskStatus status);

  List<StuckTask> prepareStuckOnProcessingTasksForResuming(String clientId, ZonedDateTime maxStuckTime);

  GetStuckTasksResponse getStuckTasks(int batchSize, TaskStatus status);

  InsertTaskResponse insertTask(InsertTaskRequest request);

  int getTasksCountInStatus(int maxCount, TaskStatus status);

  List<Pair<String, Integer>> getTasksCountInErrorGrouped(int maxCount);

  int getStuckTasksCount(ZonedDateTime age, int maxCount);

  <T> T getTask(UUID taskId, Class<T> clazz);

  DeleteFinishedOldTasksResult deleteOldTasks(TaskStatus taskStatus, Duration age, int batchSize);

  @SuppressWarnings("UnusedReturnValue")
  boolean deleteTask(UUID taskId, long version);

  boolean clearPayloadAndMarkDone(UUID taskId, long version);

  boolean setToBeRetried(UUID id, ZonedDateTime retryTime, long version, boolean resetTriesCount);

  Task grabForProcessing(BaseTask task, String nodeId, Instant maxProcessingEndTime);

  boolean setStatus(UUID taskId, TaskStatus status, long version);

  boolean markAsSubmitted(UUID taskId, long version, ZonedDateTime maxStuckTime);

  Long getTaskVersion(UUID id);

  long getApproximateTasksCount();

  long getApproximateUniqueKeysCount();
}
