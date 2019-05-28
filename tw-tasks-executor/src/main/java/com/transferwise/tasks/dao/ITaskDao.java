package com.transferwise.tasks.dao;

import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

public interface ITaskDao {

    ZonedDateTime getEarliestTaskNextEventTime(TaskStatus status);

    @Data
    @Accessors(chain = true)
    class StuckTask implements IBaseTask {
        private TaskVersionId versionId;
        private int priority;
        private String type;
        private String status;
    }

    List<StuckTask> prepareStuckOnProcessingTasksForResuming(String clientId, ZonedDateTime maxStuckTime);

    boolean scheduleTaskForImmediateExecution(UUID taskId, long version);

    GetStuckTasksResponse getStuckTasks(int batchSize, TaskStatus... statuses);

    boolean markAsSubmittedAndSetNextEventTime(TaskVersionId taskVersionId, ZonedDateTime nextEventTime);

    @Data
    @Accessors(chain = true)
    class GetStuckTasksResponse {
        private List<StuckTask> stuckTasks;
        private boolean hasMore;
    }

    InsertTaskResponse insertTask(InsertTaskRequest request);

    int getTasksCountInStatus(int maxCount, TaskStatus... statuses);

    List<Pair<String, Integer>> getTasksCountInErrorGrouped(int maxCount);

    int getStuckTasksCount(ZonedDateTime age, int maxCount);

    <T> T getTask(UUID taskId, Class<T> clazz);

    void deleteTasks(String type, String subType, TaskStatus... statuses);

    DeleteFinishedOldTasksResult deleteOldTasks(TaskStatus taskStatus, Duration age, int batchSize);

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

    boolean deleteTask(UUID taskId, long version);

    List<DaoTask1> getTasksInErrorStatus(int maxCount);

    List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount);

    boolean clearPayloadAndMarkDone(UUID taskId, long version);

    @Data
    @Accessors(chain = true)
    class DaoTask1 {
        private UUID id;
        private long version;
        private String type;
        private String subType;
        private ZonedDateTime stateTime;
    }

    List<DaoTask2> getStuckTasks(int maxCount, Duration delta);

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

    boolean setToBeRetried(UUID id, ZonedDateTime retryTime, long version, boolean resetTriesCount);

    Task grabForProcessing(BaseTask task, String nodeId, ZonedDateTime maxProcessingEndTime);

    boolean setStatus(UUID taskId, TaskStatus status, long version);

    boolean markAsSubmitted(UUID taskId, long version, ZonedDateTime maxStuckTime);

    List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses);

    void deleteAllTasks();

    Long getTaskVersion(UUID id);

    List<FullTaskRecord> getTasks(List<UUID> uuids);
}
