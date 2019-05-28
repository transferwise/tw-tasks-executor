package com.transferwise.tasks.testappa;

import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.handler.NoRetryPolicy;
import com.transferwise.tasks.handler.SimpleTaskConcurrencyPolicy;
import com.transferwise.tasks.handler.SimpleTaskProcessingPolicy;
import com.transferwise.tasks.handler.TaskHandlerAdapter;
import com.transferwise.tasks.handler.interfaces.ISyncTaskProcessor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.function.Predicate;

@Slf4j
@Component
public class TestTaskHandler extends TaskHandlerAdapter {
    private static ISyncTaskProcessor syncTaskProcessor = task -> {
        log.info("Task '" + task.getVersionId() + "' finished.");
        return null;
    };
    private static Predicate<IBaseTask> handlesPredicate = (task) -> task.getType().equals("test");

    public TestTaskHandler() {
        super(handlesPredicate, syncTaskProcessor);
    }

    public void reset() {
        setProcessor(syncTaskProcessor);
        setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.of(1, ChronoUnit.HOURS)));
        setConcurrencyPolicy(new SimpleTaskConcurrencyPolicy(1));
        setRetryPolicy(new NoRetryPolicy());
        setHandlesPredicate(handlesPredicate);
    }
}
