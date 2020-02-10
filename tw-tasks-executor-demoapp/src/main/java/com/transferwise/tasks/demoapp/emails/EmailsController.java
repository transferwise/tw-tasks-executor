package com.transferwise.tasks.demoapp.emails;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailsController {

  @Autowired
  private EmailService emailService;

  @PostMapping(value = "/v1/email/send", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public void submitPayout(@RequestBody Email email) {
    emailService.sendEmail(email);
  }
}
