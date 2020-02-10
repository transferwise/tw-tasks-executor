package com.transferwise.tasks.demoapp.emails;

import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class EmailsTaskHandlerConfiguration {

  public static final String TASK_TYPE_SEND_EMAILS = "SendEmails";

  @Autowired
  private ISyncTaskProcessor emailsSendingTaskProcessor;

  @Bean
  public ITaskHandler emailsSendingTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE_SEND_EMAILS), emailsSendingTaskProcessor)
        .setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(30))
        .setProcessingPolicy(new SimpleTaskProcessingPolicy()
            .setMaxProcessingDuration(Duration.ofMinutes(30))
            // make sure to include this bucket in the `tw-tasks.core.additional-processing-buckets` list
            .setProcessingBucket("emails"))
        .setRetryPolicy(new ExponentialTaskRetryPolicy()
            .setDelay(Duration.ofSeconds(5))
            .setMultiplier(2)
            .setMaxCount(20)
            .setMaxDelay(Duration.ofMinutes(120)));
  }
}
