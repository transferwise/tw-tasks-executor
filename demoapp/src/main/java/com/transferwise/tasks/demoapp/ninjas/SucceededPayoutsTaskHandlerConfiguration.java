package com.transferwise.tasks.demoapp.ninjas;

import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Slf4j
@Configuration
public class SucceededPayoutsTaskHandlerConfiguration {
    public static final String TASK_TYPE_PAYOUT_SUCCEEDED = "PayoutSucceeded";

    @Autowired
    private SucceededPayoutsTaskProcessor succeededPayoutsTaskProcessor;

    @Bean
    @SuppressWarnings("checkstyle:magicnumber")
    public ITaskHandler succeededPayoutsTaskHandler() {
        return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE_PAYOUT_SUCCEEDED), succeededPayoutsTaskProcessor)
            .setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(3))
            .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(30)))
            .setRetryPolicy(new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
    }
}
