package com.transferwise.tasks.management;

import com.transferwise.tasks.domain.TaskVersionId;
import lombok.Data;
import lombok.experimental.Accessors;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface ITasksManagementService {
    MarkTasksAsFailedResponse markTasksAsFailed(MarkTasksAsFailedRequest request);

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
    @SuppressWarnings("checkstyle:magicnumber")
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
    @SuppressWarnings("checkstyle:magicnumber")
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
}
