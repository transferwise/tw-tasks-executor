package com.transferwise.tasks.demoapp.slowtasks;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class SlowTasksHandlerConfiguration {

  public static final String TASK_TYPE_SLOW = "SLOW_TASK";

  @Bean
  public ITaskHandler slowTaskTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE_SLOW), (ISyncTaskProcessor) task -> ExceptionUtils.doUnchecked(() -> {
      log.info("Starting to execute slow task '" + new String(task.getData(), StandardCharsets.UTF_8) + "'.");
      Thread.sleep(60_000);
      log.info("Finished executing slow task '" + new String(task.getData(), StandardCharsets.UTF_8) + "'.");
      return null;
    }))
        .setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(5))
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(2)))
        .setRetryPolicy(
            new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
  }
}
