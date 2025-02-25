package com.transferwise.tasks;

import com.transferwise.tasks.domain.TaskContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.Future;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITasksService {

  /**
   * Adds a task for execution.
   *
   * <p>You can provide either data, which will be Json serialized into payload, or directly payload itself via dataString.
   *
   * <p>If you want a uniqueness guarantee, you can provide a unique task id (UUID) or unique key (not recommended for performance reasons).
   *
   * <p>If you want to schedule a task to run only after a specific time, provide runAfterTime value. Otherwise the task will be run immediately
   * after transaction commit. A scheduled task will be resumed by the leader node polling for it (less efficient).
   *
   * <p>Default priority is 5. The lower the number, the higher the priority, meaning the higher the chance the task will be run before other tasks.
   *
   * <p>If a task with provided id or key already exists, an ALREADY_EXISTS result is given.
   *
   * <p>warnWhenTaskExists means that warning log message will be generated if the task already exists.
   */
  AddTaskResponse addTask(AddTaskRequest request);

  @Data
  @Accessors(chain = true)
  class AddTaskRequest {

    private String type;
    private String subType;
    @SuppressFBWarnings("EI_EXPOSE_REP")
    private byte[] data;
    private UUID taskId;
    private String uniqueKey;
    private ZonedDateTime runAfterTime;
    private Integer priority;
    private boolean warnWhenTaskExists;
    private Duration expectedQueueTime;
    private CompressionRequest compression;
    private TaskContext taskContext;

    @Data
    @Accessors(chain = true)
    public static class CompressionRequest {

      private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;
      private Integer blockSizeBytes;
      private Integer level;
    }
  }

  @Data
  @Accessors(chain = true)
  class AddTaskResponse {

    private UUID taskId;
    private Result result;

    public enum Result {
      OK, ALREADY_EXISTS
    }
  }

  /**
   * Resumes a task in WAITING, NEW or SUBMITTED state. It is useful, when you want to execute an earlier scheduled task right away or you know that
   * triggering failed and don't want to wait the self healing processes kicking in.
   *
   * <p>If a task is in other states, you need to enable the force tag, because the double processing guarantees can not be given in that case.
   */
  boolean resumeTask(ResumeTaskRequest request);

  @Data
  @Accessors(chain = true)
  class ResumeTaskRequest {

    private UUID taskId;
    private long version;
    private boolean force;
  }

  /**
   * Reschedules a task in WAITING state. It is useful, when you want to change the next time the task is executed.
   *
   * <p>If the task in another state NOT_ALLOWED would be returned.
   */
  RescheduleTaskResponse rescheduleTask(RescheduleTaskRequest request);

  @Data
  @Accessors(chain = true)
  class RescheduleTaskRequest {

    private UUID taskId;
    private long version;
    private ZonedDateTime runAfterTime;
  }

  @Data
  @Accessors(chain = true)
  class RescheduleTaskResponse {

    private UUID taskId;
    private Result result;

    public enum Result {
      OK, NOT_FOUND, NOT_ALLOWED, FAILED
    }
  }

  GetTaskResponse getTask(GetTaskRequest request);

  @Data
  @Accessors(chain = true)
  class GetTaskRequest {

    private UUID taskId;
  }

  @Data
  @Accessors(chain = true)
  class GetTaskResponse {

    private UUID taskId;
    private String type;
    private long version;
    private Integer priority;
    private String status;
    private ZonedDateTime nextEventTime;

    private Result result;

    public enum Result {
      OK, NOT_FOUND
    }

  }

  void startTasksProcessing(String bucketId);

  Future<Void> stopTasksProcessing(String bucketId);

  TasksProcessingState getTasksProcessingState(String bucketId);

  enum TasksProcessingState {
    STARTED, STOPPED, STOP_IN_PROGRESS
  }
}
