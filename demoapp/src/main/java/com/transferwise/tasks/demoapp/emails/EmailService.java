package com.transferwise.tasks.demoapp.emails;

import com.transferwise.tasks.ITasksService;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(rollbackFor = Exception.class)
public class EmailService {

  @Autowired
  private ITasksService tasksService;

  public void sendEmail(Email email) {
    tasksService.addTask(new ITasksService.AddTaskRequest()
        .setTaskId(UUID.randomUUID())
        .setData(email)
        .setType(EmailsTaskHandlerConfiguration.TASK_TYPE_SEND_EMAILS));
  }
}
