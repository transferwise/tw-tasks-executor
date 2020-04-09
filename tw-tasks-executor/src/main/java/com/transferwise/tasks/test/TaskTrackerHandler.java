package com.transferwise.tasks.test;

import com.transferwise.tasks.ITasksService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TaskTrackerHandler {

  private final List<ITasksService.AddTaskRequest> requests = new ArrayList<>();

  void track(ITasksService.AddTaskRequest request) {
    requests.add(request);
  }

  public List<ITasksService.AddTaskRequest> getRequests() {
    return Collections.unmodifiableList(requests);
  }
}
