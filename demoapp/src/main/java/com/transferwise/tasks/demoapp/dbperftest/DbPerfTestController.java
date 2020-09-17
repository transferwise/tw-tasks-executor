package com.transferwise.tasks.demoapp.dbperftest;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class DbPerfTestController {

  @Autowired
  private DbPerfTestService dbPerfTestService;

  @PostMapping(value = "/v1/dbPerfTest/addTask", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public void submitSlowTask(@RequestBody int depth) {
    dbPerfTestService.addTask(depth);
  }
}
