package com.transferwise.tasks.demoapp.payout;

import com.transferwise.tasks.ITaskDataSerializer;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.demoapp.NoiseGenerator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class PayoutService {

  @Autowired
  private ITasksService tasksService;
  @Autowired
  private NoiseGenerator noiseGenerator;
  @Autowired
  private ITaskDataSerializer taskDataSerializer;

  public void submitPayout(PayoutInstruction poi) {
    poi.setNoise(noiseGenerator.generateNoise());
    tasksService.addTask(new ITasksService.AddTaskRequest()
        .setType(PayoutProcessingTaskHandlerConfiguration.TASK_TYPE_SUBMITTING)
        .setData(taskDataSerializer.serializeToJson(poi))
        .setPriority(poi.getPriority())
    );
    log.debug("Payout #" + poi.getId() + " submitted.");
  }
}
