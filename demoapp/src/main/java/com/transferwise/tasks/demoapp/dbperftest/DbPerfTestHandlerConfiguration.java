package com.transferwise.tasks.demoapp.dbperftest;

import com.transferwise.common.baseutils.ExceptionUtils;
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
public class DbPerfTestHandlerConfiguration {

  public static final String TASK_TYPE = "DB_PERF_TEST";

  @Autowired
  private DbPerfTestService dbPerfTestService;

  @Bean
  public ITaskHandler perfTestTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE), (ISyncTaskProcessor) task -> ExceptionUtils.doUnchecked(() -> {
      int depth = Integer.parseInt(task.getSubType());

      // Sleep should be added on less powerful machines to not let the load average going too high.
      // Otherwise the results are completely skewed.
      // Thread.sleep(10);

      if (depth > 1) {
        dbPerfTestService.addTask(depth - 1);
      }

      return null;
    }))
        .setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(5))
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(2)))
        .setRetryPolicy(
            new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
  }
}
