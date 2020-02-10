package com.transferwise.tasks.demoapp.emails;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class EmailsSendingTaskProcessor implements ISyncTaskProcessor {

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public ProcessResult process(ITask task) {
    ExceptionUtils.doUnchecked(() -> {
      Email email = objectMapper.readValue(task.getData(), Email.class);

      log.debug("Sent email #" + email.getId() + ".");
    });
    return null;
  }
}
