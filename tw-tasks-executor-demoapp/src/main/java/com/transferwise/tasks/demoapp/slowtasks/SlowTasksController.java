package com.transferwise.tasks.demoapp.slowtasks;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class SlowTasksController {

  @Autowired
  private SlowTasksService slowTasksService;

  @PostMapping(value = "/v1/slowtasks/submit", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public void submitSlowTask(@RequestBody String data) {
    slowTasksService.submitSlowTask(data);
  }
}
