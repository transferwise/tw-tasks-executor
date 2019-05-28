package com.transferwise.tasks.demoapp.emails;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EmailsController {
    @Autowired
    private EmailService emailService;

    @RequestMapping(value = "/v1/email/send", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public void submitPayout(@RequestBody Email email) {
        emailService.sendEmail(email);
    }
}
