## Priorities

Please refer to [algorithms.md](algorithms.md) for details about the implementation.

### Risks

Using priorities can have some risks:
- inability of tw-tasks to commit offsets
- increase lag in trigger topic
- OOM caused by tracking entries related to tasks that were handled but not commited yet.

### Risk-free usage of priorities

1. create one shard per priority.

All tasks within one shard should have the same priority.

```java
public class SomeTaskHandler implements ITaskHandler {
  @Override
  public ITaskProcessingPolicy getProcessingPolicy(IBaseTask task) {
    return new SimpleTaskProcessingPolicy()
        //other config..
        .setProcessingBucket(taskBucketMapper.getBucket(task));
  }

}

public class TaskBucketMapper {
  public String getBucket(IBaseTask task) {
    if (task.getPriority() < 5) {
      return "highBucket";
    } else {
      return "lowBucket";
    }
  }
}

```

2. Isolate concurrency policies per priority/shard

Below, a simple example showcasing isolation of the concurrency policy per priority.
```Java

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskConcurrencyPolicy implements ITaskConcurrencyPolicy {
  private static final int MAX_SLOTS = 5; // 5 slots for all priorities
  ConcurrentHashMap<PriorityEnum, AtomicInteger> prioritySlots = new ConcurrentHashMap<>();

  @Override
  public BookSpaceResponse bookSpace(IBaseTask task) {
    var priority = PriorityEnum.toEnum(task.getPriority());
    if (prioritySlots.computeIfAbsent(priority, k -> new AtomicInteger()).getAndIncrement() >= MAX_SLOTS) {
      prioritySlots.get(priority).decrementAndGet();
      return new BookSpaceResponse(false);
    }

    return new BookSpaceResponse(true);
  }

  @Override
  public void freeSpace(IBaseTask task) {
    var priority = PriorityEnum.toEnum(task.getPriority());
    prioritySlots.get(priority).decrementAndGet();
    prioritySlots.get(priority).decrementAndGet();
    if (inProgressForType.decrementAndGet() < 0) {
      throw new IllegalStateException("Counter went below zero. Algorithm error detected.");
    }
  }
}

enum PriorityEnum {
  HIGH,
  LOW;

  public static PriorityEnum toEnum(int priority) {
    if (priority < 5) return HIGH;
    return LOW;
  }
}

```

In a real-life scenario, we would want more slots for high priority.