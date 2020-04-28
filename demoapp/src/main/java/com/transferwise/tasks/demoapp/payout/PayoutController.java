package com.transferwise.tasks.demoapp.payout;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class PayoutController {

  @Autowired
  private PayoutService payoutService;

  @PostMapping(value = "/v1/payout/submit", produces = MediaType.APPLICATION_JSON_VALUE, consumes = MediaType.APPLICATION_JSON_VALUE)
  public void submitPayout(@RequestBody PayoutInstruction payout) {
    payoutService.submitPayout(payout);
  }

}
