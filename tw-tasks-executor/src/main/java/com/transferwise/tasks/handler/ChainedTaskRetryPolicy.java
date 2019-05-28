package com.transferwise.tasks.handler;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.handler.interfaces.ITaskRetryPolicy;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.time.ZonedDateTime;
import java.util.List;

@Getter
@Setter
@Accessors(chain = true)
public class ChainedTaskRetryPolicy implements ITaskRetryPolicy {
    private List<ITaskRetryPolicy> retryPolicies;

    @Override
    public ZonedDateTime getRetryTime(ITask task, Throwable t) {
        for (ITaskRetryPolicy policy : retryPolicies) {
            ZonedDateTime retryTime = policy.getRetryTime(task, t);
            if (retryTime != null) {
                return retryTime;
            }
        }
        return null;
    }
}
