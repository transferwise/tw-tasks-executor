package com.transferwise.tasks.testappa;

import com.transferwise.tasks.domain.ITask;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

@Component
@Transactional(rollbackFor = Exception.class)
@Slf4j
public class ResultRegisteringSyncTaskProcessor implements IResultRegisteringSyncTaskProcessor {
    @Getter
    @Setter
    private Map<UUID, Boolean> taskResults = new ConcurrentHashMap<>();

    @Setter
    @Getter
    private Predicate<ITask> resultPredicate;

    @Override
    public void reset() {
        taskResults.clear();
        resultPredicate = null;
    }

    @Override
    public ProcessResult process(ITask task) {
        boolean result = resultPredicate == null || resultPredicate.test(task);
        taskResults.put(task.getVersionId().getId(), result);
        log.info("Task " + task.getVersionId().getId() + " got processed.");
        return null;
    }
}
