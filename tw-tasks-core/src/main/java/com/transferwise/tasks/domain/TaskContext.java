package com.transferwise.tasks.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TaskContext {

  public static final TaskContext EMPTY = new TaskContext();

  private Map<String, Object> contextMap = new HashMap<>();

  public <T> T get(String key, Class<T> cls) {
    return Optional.ofNullable(contextMap.get(key)).map(cls::cast).orElse(null);
  }

  public void merge(TaskContext taskContext) {
    if (taskContext != null) {
      if (taskContext.contextMap != null) {
        contextMap.putAll(taskContext.contextMap);
      }
    }
  }
}
