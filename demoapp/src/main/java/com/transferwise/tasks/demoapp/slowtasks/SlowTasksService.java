package com.transferwise.tasks.demoapp.slowtasks;

import com.transferwise.tasks.ITasksService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class SlowTasksService {

  @Autowired
  private ITasksService tasksService;

  public void submitSlowTask(String data) {
    log.info("Submitting slow task.");

    tasksService.addTask(new ITasksService.AddTaskRequest().setDataString(data).setType(SlowTasksHandlerConfiguration.TASK_TYPE_SLOW));
  }
}
