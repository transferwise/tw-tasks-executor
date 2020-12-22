package com.transferwise.tasks.demoapp.slowtasks;

import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
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
  @Autowired
  private ITaskDataSerializer taskDataSerializer;

  public void submitSlowTask(String data) {
    log.info("Submitting slow task.");

    tasksService.addTask(new AddTaskRequest()
        .setData(taskDataSerializer.serialize(data)).setType(SlowTasksHandlerConfiguration.TASK_TYPE_SLOW));
  }
}
