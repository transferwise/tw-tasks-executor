package com.transferwise.tasks.domain;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.UUID;

@Data
@Accessors(chain = true)
public class BaseTask implements IBaseTask {
    private UUID id;
    private String type;
    private long version;
    private int priority;

    public ITaskVersionId getVersionId() {
        return new TaskVersionId(id, version);
    }
}
