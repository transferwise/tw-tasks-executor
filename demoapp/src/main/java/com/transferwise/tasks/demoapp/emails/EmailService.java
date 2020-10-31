package com.transferwise.tasks.demoapp.emails;

import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.demoapp.NoiseGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(rollbackFor = Exception.class)
public class EmailService {

  @Autowired
  private ITasksService tasksService;

  @Autowired
  private NoiseGenerator noiseGenerator;

  public void sendEmail(Email email) {
    email.setNoise(noiseGenerator.generateNoise());
    tasksService.addTask(new ITasksService.AddTaskRequest()
        .setData(email)
        .setType(EmailsTaskHandlerConfiguration.TASK_TYPE_SEND_EMAILS));
  }
}
