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

2. isolate concurrency policies per priority/shard

Having the same slots with different priorities/shards, can cause high priority tasks processing to stop the processing of low priority ones.

If high priority tasks have a high enough throughput, it would monopolise the concurrency slots.

The consequence of this, is that triggers might not be acknowledged, causing high offset count tracking and delay in commiting offsets (on impacted shard) which might cause kafka consumer re-balances (if consumer for the
impacted shard doesn't send heat-beats. This is an issue, as of Feb 2025).

By isolating the concurrency policies, we make sure that the lower priority tasks would have a chance to run.

Below, a simple example showcasing isolation of the concurrency policy per priority.

```Java

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class TaskConcurrencyPolicy implements ITaskConcurrencyPolicy {
  // consider more slots for high priority, in a real-life scenario
  private static final int MAX_SLOTS = 5; // 5 slots per priority
  ConcurrentHashMap<PriorityEnum, AtomicInteger> prioritySlots = new ConcurrentHashMap<>();

  @Override
  public BookSpaceResponse bookSpace(IBaseTask task) {
    var priority = PriorityEnum.toEnum(task.getPriority());
    if (prioritySlots.computeIfAbsent(priority, k -> new AtomicInteger()).getAndIncrement() > MAX_SLOTS) {
      prioritySlots.get(priority).decrementAndGet();
      return new BookSpaceResponse(false);
    }

    return new BookSpaceResponse(true);
  }

  @Override
  public void freeSpace(IBaseTask task) {
    var priority = PriorityEnum.toEnum(task.getPriority());
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