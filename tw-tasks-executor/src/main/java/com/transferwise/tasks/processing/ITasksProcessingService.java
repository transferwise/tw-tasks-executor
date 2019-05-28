package com.transferwise.tasks.processing;

import com.transferwise.tasks.triggering.TaskTriggering;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.function.Consumer;

public interface ITasksProcessingService {
    AddTaskForProcessingResponse addTaskForProcessing(TaskTriggering taskTriggering);

    @Data
    @Accessors(chain = true)
    class AddTaskForProcessingResponse {
        private ResultCode result;

        public enum ResultCode {
            OK, FULL
        }
    }

    void addTaskTriggeringFinishedListener(Consumer<TaskTriggering> consumer);
}
