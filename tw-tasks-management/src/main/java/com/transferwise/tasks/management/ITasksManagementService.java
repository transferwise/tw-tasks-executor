package com.transferwise.tasks.management;

import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.entrypoints.EntryPoint;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskDataResponse;
import com.transferwise.tasks.management.ITasksManagementPort.GetTaskWithoutDataResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;

public interface ITasksManagementService {

  MarkTasksAsFailedResponse markTasksAsFailed(MarkTasksAsFailedRequest request);

  @EntryPoint(usesExisting = true)
  GetTaskWithoutDataResponse getTaskWithoutData(UUID taskId);

  GetTaskDataResponse getTaskData(GetTaskDataRequest taskDataRequest);

  @Data
  @Accessors(chain = true)
  class GetTaskDataRequest {

    private UUID taskId;
    private ContentFormat contentFormat = ContentFormat.UTF8_STRING;

    public enum ContentFormat {
      UTF8_STRING,
      BASE64;

      private static final Map<String, ContentFormat> index = Arrays.stream(values()).collect(Collectors.toMap(e -> e.name().toLowerCase(), e -> e));

      public static ContentFormat of(String value) {
        if (value == null) {
          return null;
        }
        return index.get(value.toLowerCase());
      }
    }
  }

  @Data
  @Accessors(chain = true)
  class MarkTasksAsFailedRequest {

    private List<TaskVersionId> taskVersionIds = new ArrayList<>();
  }

  @Data
  @Accessors(chain = true)
  class MarkTasksAsFailedResponse {

    private Map<UUID, Result> results = new HashMap<>();

    @Data
    @Accessors(chain = true)
    public static class Result {

      private boolean success;
      private String message;
    }
  }

  ResumeTasksImmediatelyResponse resumeTasksImmediately(ResumeTasksImmediatelyRequest request);

  @Data
  @Accessors(chain = true)
  class ResumeTasksImmediatelyRequest {

    private List<TaskVersionId> taskVersionIds = new ArrayList<>();
  }

  ResumeTasksImmediatelyResponse resumeAllTasksImmediately(ResumeAllTasksImmediatelyRequest request);

  @Data
  @Accessors(chain = true)
  class ResumeAllTasksImmediatelyRequest {

    private String taskType;
    private int maxCount = 100;
  }

  @Data
  @Accessors(chain = true)
  class ResumeTasksImmediatelyResponse {

    private Map<UUID, Result> results = new HashMap<>();

    @Data
    @Accessors(chain = true)
    public static class Result {

      private boolean success;
      private String message;
    }
  }

  GetTasksInErrorResponse getTasksInError(GetTasksInErrorRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksInErrorRequest {

    private int maxCount;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksInErrorResponse {

    private List<TaskInError> tasksInError = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class TaskInError {

      private TaskVersionId taskVersionId;
      private String type;
      private String subType;
      private Instant errorTime;
    }
  }

  GetTasksStuckResponse getTasksStuck(GetTasksStuckRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksStuckRequest {

    private int maxCount;
    private Duration delta;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksStuckResponse {

    private List<TaskStuck> tasksStuck = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class TaskStuck {

      private TaskVersionId taskVersionId;
      private Instant stuckTime;
    }
  }

  GetTasksInProcessingOrWaitingResponse getTasksInProcessingOrWaiting(GetTasksInProcessingOrWaitingRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksInProcessingOrWaitingRequest {

    private int maxCount = 10;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksInProcessingOrWaitingResponse {

    private List<TaskInProcessingOrWaiting> tasksInProcessingOrWaiting = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class TaskInProcessingOrWaiting {

      private TaskVersionId taskVersionId;
      private String type;
      private String subType;
      private String status;
      private Instant stateTime;
    }
  }

  GetTasksByIdResponse getTasksById(GetTasksByIdRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksByIdRequest {

    private List<UUID> taskIds = new ArrayList<>();
  }

  @Data
  @Accessors(chain = true)
  class GetTasksByIdResponse {

    private List<Task> tasks = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class Task {

      private TaskVersionId taskVersionId;
      private String type;
      private String subType;
      private String status;
      private Instant stateTime;
    }
  }

  GetTasksByTypeResponse getTasksByType(GetTasksByTypeRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksByTypeRequest {

    private List<String> taskTypes = new ArrayList<>();
  }

  @Data
  @Accessors(chain = true)
  class GetTasksByTypeResponse {

    private List<Task> tasks = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class Task {

      private TaskVersionId taskVersionId;
      private String type;
      private String subType;
      private String status;
      private Instant stateTime;
    }
  }
}
