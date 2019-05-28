package com.transferwise.tasks.demoapp.ninjas;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.demoapp.payout.PayoutInstruction;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class SucceededPayoutsTaskProcessor implements ISyncTaskProcessor {
    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public ProcessResult process(ITask task) {
        ExceptionUtils.doUnchecked(() -> {
            PayoutInstruction poi = objectMapper.readValue(task.getData(), PayoutInstruction.class);

            log.debug("Poi #" + poi.getId() + " succeeded.");
        });
        return null;
    }
}
