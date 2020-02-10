package com.transferwise.tasks.demoapp.payout;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class PayoutSubmittingTaskProcessor implements ISyncTaskProcessor {

  @Autowired
  private ObjectMapper objectMapper;
  @Autowired
  private ITasksService tasksService;

  @Override
  public ProcessResult process(ITask task) {
    ExceptionUtils.doUnchecked(() -> {
      // Lets create more tasks for performance tests. So that initiating payouts over REST would not be the bottleneck.
      for (int i = 0; i < 10; i++) {
        PayoutInstruction poi = objectMapper.readValue(task.getData(), PayoutInstruction.class);

        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setType(PayoutProcessingTaskHandlerConfiguration.TASK_TYPE_PROCESSING + "." + poi.getType())
            .setTaskId(UUID.randomUUID())
            .setData(poi)
            .setPriority(poi.getPriority())
        );

        log.debug("Submitted payout #" + poi.getId() + " for " + poi.getType());
      }
    });
    return null;
  }
}
