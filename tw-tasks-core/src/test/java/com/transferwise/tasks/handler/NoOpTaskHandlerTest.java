package com.transferwise.tasks.handler;


import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor.ProcessResult.ResultCode;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class NoOpTaskHandlerTest {

  @Test
  void whenNoTaskTypeIsHandledByNoOpTaskHandler_thenCantHandle() {
    NoOpTaskHandler noOpTaskHandler = new NoOpTaskHandler(List.of());
    IBaseTask task = new BaseTask().setType("SOME_TASK_TYPE");
    Assertions.assertThat(noOpTaskHandler.handles(task)).isFalse();
  }

  @Test
  void whenTaskTypeIsNotHandledByNoOpTaskHandler_thenCantHandle() {
    NoOpTaskHandler noOpTaskHandler = new NoOpTaskHandler(List.of("SOME_TASK_TYPE"));
    IBaseTask task = new BaseTask().setType("SOME_OTHER_TASK_TYPE");
    Assertions.assertThat(noOpTaskHandler.handles(task)).isFalse();
  }

  @Test
  void whenTaskTypeIsHandledByNoOpTaskHandler_thenCanHandle() {
    NoOpTaskHandler noOpTaskHandler = new NoOpTaskHandler(List.of("SOME_TASK_TYPE"));
    IBaseTask task = new BaseTask().setType("SOME_TASK_TYPE");
    Assertions.assertThat(noOpTaskHandler.handles(task)).isTrue();
  }

  @Test
  void processReturnsASuccessfulResult() {
    NoOpTaskHandler noOpTaskHandler = new NoOpTaskHandler(List.of("SOME_TASK_TYPE"));
    ITask task = new Task().setType("SOME_TASK_TYPE");
    Assertions.assertThat(noOpTaskHandler.process(task).getResultCode()).isEqualTo(ResultCode.DONE);
  }
}