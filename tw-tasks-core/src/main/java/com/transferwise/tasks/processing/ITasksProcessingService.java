package com.transferwise.tasks.processing;

import com.transferwise.tasks.triggering.TaskTriggering;
import java.util.function.Consumer;
import lombok.Data;
import lombok.experimental.Accessors;

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

  void startProcessing();
}
