package com.transferwise.tasks.processing;

import com.transferwise.tasks.triggering.TaskTriggering;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class GlobalProcessingState {

  private Map<String, Bucket> buckets = new ConcurrentHashMap<>();

  public void increaseBucketsVersion() {
    buckets.forEach((k, v) -> v.increaseVersion());
  }

  @Data
  @Accessors(chain = true)
  public static class Bucket {

    private String bucketId;
    private Lock versionLock = new ReentrantLock();
    private Condition versionCondition = versionLock.newCondition();
    private AtomicLong version = new AtomicLong(0);
    private Set<Integer> priorities = new TreeSet<>();
    private Map<Integer, PrioritySlot> prioritySlots = new ConcurrentHashMap<>();
    private AtomicInteger size = new AtomicInteger();
    private AtomicInteger runningTasksCount = new AtomicInteger();
    private AtomicInteger inProgressTasksGrabbingCount = new AtomicInteger();
    private Semaphore tasksGrabbingSemaphore;

    public Bucket(int minPriority, int maxPriority) {
      for (int i = minPriority; i <= maxPriority; i++) {
        addPriority(i);
      }
    }

    // Optimization to avoid waiting behind a lock.
    // One ongoing version update is enough to wake up all necessary components.
    private AtomicBoolean versionUpdateInProgress = new AtomicBoolean();

    public void increaseVersion() {
      if (versionUpdateInProgress.getAndSet(true)) {
        return;
      }

      versionLock.lock();
      try {
        version.incrementAndGet();
        versionCondition.signalAll();
      } finally {
        // Needs to happen before we unlock.
        versionUpdateInProgress.getAndSet(false);
        versionLock.unlock();
      }
    }

    public synchronized void addPriority(Integer priority) {
      if (priorities.contains(priority)) {
        return;
      }
      priorities.add(priority);
      prioritySlots.put(priority, new PrioritySlot());
    }

    public synchronized PrioritySlot getPrioritySlot(Integer priority) {
      return prioritySlots.get(priority);
    }
  }

  @Data
  @Accessors(chain = true)
  public static class PrioritySlot {

    private Set<TypeTasks> orderedTypeTasks = new TreeSet<>((ttA, ttB) -> {
      if (ttA == ttB) {
        return 0;
      }
      TaskTriggering ttAPeek = ttA.peek();
      TaskTriggering ttBPeek = ttB.peek();
      if (ttAPeek == null && ttBPeek != null) {
        return 1;
      } else if (ttAPeek != null && ttBPeek == null) {
        return -1;
      } else if (ttAPeek == null || ttAPeek.getSequence() == ttBPeek.getSequence()) {
        return ttA.getType().compareTo(ttB.getType());
      }
      return ttA.getType().compareTo(ttB.getType());
    });

    private Map<String, TypeTasks> typeTasks = new HashMap<>();

    /**
     * Intermediate unstructured buffer to reduce the need to lock the whole prioritySlot.
     */
    private Queue<TaskTriggering> taskTriggerings = new ConcurrentLinkedQueue<>();
  }

  @Data
  @Accessors(chain = true)
  public static class TypeTasks {

    private String type;
    private AtomicInteger size = new AtomicInteger();
    private Queue<TaskTriggering> tasks = new ConcurrentLinkedQueue<>();

    public TaskTriggering peek() {
      return tasks.peek();
    }

    public void removeLast() {
      TaskTriggering taskTriggering = tasks.poll();
      if (taskTriggering != null) {
        size.decrementAndGet();
      }
    }

    public void add(TaskTriggering taskTriggering) {
      if (tasks.add(taskTriggering)) {
        size.incrementAndGet();
      }
    }
  }
}



