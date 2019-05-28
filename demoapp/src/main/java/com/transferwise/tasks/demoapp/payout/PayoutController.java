package com.transferwise.tasks.demoapp.payout;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class PayoutController {
    @Autowired
    private PayoutService payoutService;

    @RequestMapping(value = "/v1/payout/submit", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
    public void submitPayout(@RequestBody PayoutInstruction payout) {
        payoutService.submitPayout(payout);
    }

}
