package com.transferwise.tasks.demoapp.slowtasks;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class SlowTasksController {
    @Autowired
    private SlowTasksService slowTasksService;

    @RequestMapping(value = "/v1/slowtasks/submit", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public void submitSlowTask(@RequestBody String data) {
        slowTasksService.submitSlowTask(data);
    }
}
