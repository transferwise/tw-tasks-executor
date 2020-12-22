package com.transferwise.tasks.demoapp.payout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class PayoutProcessingTaskProcessor implements ISyncTaskProcessor {

  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private KafkaTemplate<String, byte[]> kafkaTemplate;

  @Override
  public ProcessResult process(ITask task) {
    ExceptionUtils.doUnchecked(() -> {
      PayoutInstruction poi = objectMapper.readValue(task.getData(), PayoutInstruction.class);

      /*
      if (poi.getType().equals("LHV")) {
        Thread.sleep(10000);
      } else {
        Thread.sleep(2000);
      }
      */

      kafkaTemplate.send("payout.succeeded", task.getData());
      log.debug("Processed payout #" + poi.getId() + " for " + poi.getType());
    });
    return null;
  }
}
