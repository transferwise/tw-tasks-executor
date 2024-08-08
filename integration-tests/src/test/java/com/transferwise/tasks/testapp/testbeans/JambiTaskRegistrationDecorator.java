package com.transferwise.tasks.testapp.testbeans;

import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.domain.TaskContext;
import com.transferwise.tasks.processing.ITaskRegistrationDecorator;
import java.util.Map;
import org.springframework.stereotype.Component;

@Component
public class JambiTaskRegistrationDecorator implements ITaskRegistrationDecorator {

  @Override
  public AddTaskRequest intercept(AddTaskRequest request) {
    if ("Jambi".equals(request.getSubType())) {
      return request.setTaskContext(new TaskContext().setContextMap(Map.of("adam-jones", "eulogy")));
    }
    return request;
  }
}
