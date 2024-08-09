package com.transferwise.tasks.processing;

import com.transferwise.tasks.ITasksService.AddTaskRequest;

public interface ITaskRegistrationDecorator {

  AddTaskRequest decorate(AddTaskRequest request);
}
