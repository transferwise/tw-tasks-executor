package com.transferwise.tasks.processing;

import com.transferwise.tasks.triggering.TaskTriggering;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Data
@Accessors(chain = true)
public class ProcessingState {
    private Map<String, Bucket> buckets = new ConcurrentHashMap<>();

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
        private Lock tasksGrabbingLock = new ReentrantLock();
        private Condition tasksGrabbingCondition = tasksGrabbingLock.newCondition();

        public Bucket(int minPriority, int maxPriority) {
            for (int i = minPriority; i < maxPriority; i++) {
                addPriority(i);
            }
        }

        public void increaseVersion() {
            versionLock.lock();
            try {
                version.incrementAndGet();
                versionCondition.signalAll();
            } finally {
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
        private Set<TypeTasks> orderedTypeTasks = new TreeSet<>((a, b) -> {
            if (a == b) {
                return 0;
            }
            TaskTriggering aPeek = a.getTasks().peek();
            TaskTriggering bPeek = b.getTasks().peek();
            if (aPeek == null && bPeek != null) {
                return 1;
            }
            else if (aPeek != null && bPeek == null) {
                return -1;
            }
            else if (aPeek == null || aPeek.getSequence() == bPeek.getSequence()) {
                return a.getType().compareTo(b.getType());
            }
            return a.getType().compareTo(b.getType());
        });

        private Map<String, TypeTasks> typeTasks = new HashMap<>();

        /**
         * Intermediate unstructured buffer to reduce the need to lock the whole prioritySlot;
         */
        private Queue<TaskTriggering> taskTriggerings = new ConcurrentLinkedQueue<>();
    }

    @Data
    @Accessors(chain = true)
    public static class TypeTasks {
        private String type;
        private Queue<TaskTriggering> tasks = new ConcurrentLinkedQueue<>();
    }
}



