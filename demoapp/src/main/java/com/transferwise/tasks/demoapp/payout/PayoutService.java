package com.transferwise.tasks.demoapp.payout;

import com.transferwise.tasks.ITasksService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class PayoutService {
    @Autowired
    private ITasksService tasksService;

    public void submitPayout(PayoutInstruction poi) {
        tasksService.addTask(new ITasksService.AddTaskRequest()
            .setType(PayoutProcessingTaskHandlerConfiguration.TASK_TYPE_SUBMITTING)
            .setTaskId(UUID.randomUUID())
            .setData(poi)
            .setPriority(poi.getPriority())
        );
        log.debug("Payout #" + poi.getId() + " submitted.");
    }
}
