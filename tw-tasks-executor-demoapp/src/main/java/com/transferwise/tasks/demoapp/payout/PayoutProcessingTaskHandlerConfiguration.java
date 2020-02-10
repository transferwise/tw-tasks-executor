package com.transferwise.tasks.demoapp.payout;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.ExponentialTaskRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import com.transferwise.tasks.handler.interfaces.ITaskConcurrencyPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class PayoutProcessingTaskHandlerConfiguration {

  public static final String TASK_TYPE_PROCESSING = "PayoutProcessing";
  public static final String TASK_TYPE_SUBMITTING = "PayoutSubmitting";

  @Autowired
  private ISyncTaskProcessor payoutProcessingTaskProcessor;

  @Autowired
  private ISyncTaskProcessor payoutSubmittingTaskProcessor;

  @Bean
  public ITaskHandler payoutProcessingTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE_PROCESSING), payoutProcessingTaskProcessor)
        .setConcurrencyPolicy(taskConcurrencyPolicy())
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(30)))
        .setRetryPolicy(
            new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
  }

  @Bean
  public ITaskHandler payoutSubmittingTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith(TASK_TYPE_SUBMITTING), payoutSubmittingTaskProcessor)
        .setConcurrencyPolicy(taskConcurrencyPolicy())
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(30)))
        .setRetryPolicy(
            new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
  }

  @Bean
  public ITaskConcurrencyPolicy taskConcurrencyPolicy() {
    return new ITaskConcurrencyPolicy() {
      private AtomicInteger totalInProgressCnt = new AtomicInteger();
      private AtomicInteger lhvInProgressCnt = new AtomicInteger();

      @Override
      public boolean bookSpaceForTask(IBaseTask task) {
        if (totalInProgressCnt.incrementAndGet() > 40) {
          totalInProgressCnt.decrementAndGet();
          return false;
        }

        if (task.getType().equals(TASK_TYPE_PROCESSING + ".LHV")) {
          if (lhvInProgressCnt.incrementAndGet() > 20) {
            lhvInProgressCnt.decrementAndGet();
            totalInProgressCnt.decrementAndGet();
            return false;
          }
        }
        return true;
      }

      @Override
      public void freeSpaceForTask(IBaseTask task) {
        totalInProgressCnt.decrementAndGet();
        if (task.getType().equals(TASK_TYPE_PROCESSING + ".LHV")) {
          lhvInProgressCnt.decrementAndGet();
        }
      }
    };
  }
}
