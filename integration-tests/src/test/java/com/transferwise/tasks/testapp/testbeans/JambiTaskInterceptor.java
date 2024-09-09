package com.transferwise.tasks.testapp.testbeans;

import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.processing.ITaskProcessingInterceptor;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JambiTaskInterceptor implements ITaskProcessingInterceptor {

  private final MeterRegistry meterRegistry;

  @Override
  public void doProcess(Task task, Runnable processor) {
    if ("Jambi".equals(task.getSubType())) {
      if (task.getTaskContext() != null) {
        var name = task.getTaskContext().get("adam-jones", String.class);
        meterRegistry.counter("tool", "song", name).increment();
      }
    }
    processor.run();
  }
}
