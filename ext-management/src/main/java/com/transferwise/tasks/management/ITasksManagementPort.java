package com.transferwise.tasks.management;

import com.transferwise.tasks.domain.TaskVersionId;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

public interface ITasksManagementPort {

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/markTasksAsFailed", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<MarkTasksAsFailedResponse> markTasksAsFailed(@RequestBody MarkTasksAsFailedRequest request);

  @Data
  @Accessors(chain = true)
  class MarkTasksAsFailedRequest {

    private List<TaskVersionId> taskVersionIds = new ArrayList<>();

    public MarkTasksAsFailedRequest addTaskVersionId(TaskVersionId taskVersionId) {
      taskVersionIds.add(taskVersionId);
      return this;
    }
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

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/resumeTasksImmediately", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<ResumeTasksImmediatelyResponse> resumeTasksImmediately(@RequestBody ResumeTasksImmediatelyRequest request);

  @Data
  @Accessors(chain = true)
  class ResumeTasksImmediatelyRequest {

    private List<TaskVersionId> taskVersionIds = new ArrayList<>();

    public ResumeTasksImmediatelyRequest addTaskVersionId(TaskVersionId taskVersionId) {
      taskVersionIds.add(taskVersionId);
      return this;
    }
  }

  /**
   * Resumes immediately all tasks in ERROR state of the specified type. TODO: Add support for resuming tasks not in ERROR state (be aware of double
   * processing risk).
   */
  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/resumeAllTasksImmediately", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<ResumeTasksImmediatelyResponse> resumeAllTasksImmediately(@RequestBody ResumeAllTasksImmediatelyRequest request);

  @Data
  @Accessors(chain = true)
  class ResumeAllTasksImmediatelyRequest {

    /**
     * All tasks of this type will be resumed (up to maxCount).
     */
    private String taskType;
    private Integer maxCount;
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

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/getTasksInError", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<GetTasksInErrorResponse> getTasksInError(@RequestBody(required = false) GetTasksInErrorRequest request);

  //TODO: Move to RPC form, REST sucks.
  //      add it to UI
  @GetMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/task/{taskId}/noData", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<TaskWithoutData> getTask(@PathVariable final String taskId);

  @GetMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/task/{taskId}/data", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<GetTaskDataResponse> getTaskData(@PathVariable final UUID taskId);

  @Data
  @Accessors(chain = true)
  class GetTaskDataResponse {

    private String data;
    private ResultCode resultCode;

    public enum ResultCode {
      SUCCESS, NOT_FOUND
    }
  }

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/getTasksInProcessingOrWaiting", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<GetTasksInProcessingOrWaitingResponse> getTasksInProcessingOrWaiting(
      @RequestBody(required = false) GetTasksInProcessingOrWaitingRequest request);

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/getTasksById", produces = {MediaType.APPLICATION_JSON_VALUE})
  @ResponseBody
  ResponseEntity<GetTasksByIdResponse> getTasksById(@RequestBody GetTasksByIdRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksByIdRequest {

    @NotNull
    private List<UUID> taskIds;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksByIdResponse {

    private List<Task> taskList;

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

  @Data
  @Accessors(chain = true)
  @SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
  class TaskWithoutData {

    // keep it without PII (task data)
    private String id;
    private long version;
    private String type;
    private String subType;
    private String status;
    private long processingTriesCount;
    private int priority;
    private Date stateTime;
    private Date nextEventTime;
    private String processingClientId;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksInErrorRequest {

    private int maxCount;
  }

  @Data
  @Accessors(chain = true)
  class GetTasksInProcessingOrWaitingRequest {

    private int maxCount;
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

  @Data
  @Accessors(chain = true)
  class GetTasksInErrorResponse {

    private List<TaskInError> tasksInError = new ArrayList<>();

    @Data
    @Accessors(chain = true)
    public static class TaskInError {

      private TaskVersionId taskVersionId;
      private Instant errorTime;
      private String type;
      private String subType;
    }
  }

  @PostMapping(value = "${tw-tasks.core.base-url:}/v1/twTasks/getTasksStuck", produces = MediaType.APPLICATION_JSON_VALUE)
  @ResponseBody
  ResponseEntity<GetTasksStuckResponse> getTasksStuck(@RequestBody(required = false) GetTasksStuckRequest request);

  @Data
  @Accessors(chain = true)
  class GetTasksStuckRequest {

    private int maxCount;
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
}
