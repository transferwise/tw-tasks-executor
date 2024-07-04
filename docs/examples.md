# Examples

1. Setting up a task handler.
   A Bean implementing `ITaskHandler` needs to be defined.
   For simplicity we use `TaskHandlerAdapter`.
```java
@Bean
public ITaskHandler executePayoutTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith("EXECUTE_PAYOUT"), (ISyncTaskProcessor) task -> {
        ExecutePayoutCommand command = jsonConverter.toObject(task.getData(), ExecutePayoutCommand.class);
        payoutService.executePayout(command);
        return new ISyncTaskProcessor.ProcessResult().setResultCode(ISyncTaskProcessor.ProcessResult.ResultCode.DONE);
    })
    .setConcurrencyPolicy(payoutTaskConcurrencyPolicy)
    .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(30))
            // make sure to include this bucket in the `tw-tasks.core.additional-processing-buckets` list
            .setProcessingBucket("payout"))
    // 5s 20s 1m20s 5m20s 20m
    .setRetryPolicy(new ExponentialTaskRetryPolicy()
            .setDelay(Duration.ofSeconds(5))
            .setMultiplier(4)
            .setMaxCount(5)
            .setMaxDelay(Duration.ofMinutes(20)));
}
```
2. Setting up a task for execution.
   Let's assume, we want a payout done after 1 hour.
```java
tasksService.addTask(new ITasksService.AddTaskRequest().setType("EXECUTE_PAYOUT")
.setData(executePayoutCommand).setRunAfterTime(ZonedDateTime.now().plusHours(1)));
```
3. Implementing a specific ITaskConcurrencyPolicy.
   Let's assume, we want to have maximum of 5 payouts per node, but not more than 2 per partner.
   Our task types have partner info inside, for example `EXECUTE_PAYOUT|PARTNERY`.

Groovy code:
```groovy
class PayoutTaskConcurrencyPolicy implements ITaskConcurrencyPolicy {
	private AtomicInteger inProgressCnt = new AtomicInteger()

	private Map<String, AtomicInteger> inProgressCntsPerType = new ConcurrentHashMap<>()

	@Override
	BookSpaceResponse bookSpace(BaseTask task) {
		int maxConcurrency = 5

		int cnt = inProgressCnt.incrementAndGet();
		if (cnt > maxConcurrency) {
			inProgressCnt.decrementAndGet();
			return new BookSpaceResponse(false);
		}

		// Overall concurrency is satisfied, lets check per partner concurrency as well.
		String transferMethod = getTransferMethod(task);
		if (transferMethod != null) {
			int maxConcurrencyPerTransferMethod = 2

			AtomicInteger inProgressCntPerType = getOrCreateTypeCounter(transferMethod);
			if (inProgressCntPerType.incrementAndGet() > maxConcurrencyPerTransferMethod) {
				inProgressCntPerType.decrementAndGet();
				inProgressCnt.decrementAndGet();
				return new BookSpaceResponse(false);
			}
		}
		return new BookSpaceResponse(true);
	}

	@Override
	void freeSpace(BaseTask task) {
		String transferMethod = getTransferMethod(task);
		if (transferMethod != null) {
			if (getOrCreateTypeCounter(transferMethod).decrementAndGet() < 0) {
				throw new IllegalStateException("Type counter for '" + transferMethod + "' went below zero. Algorithm error detected.");
			}
		}

		if (inProgressCnt.decrementAndGet() < 0) {
			throw new IllegalStateException("Counter went below zero. Algorithm error detected.");
		}
	}

	private AtomicInteger getOrCreateTypeCounter(String type) {
		AtomicInteger counter = inProgressCntsPerType.get(type);
		if (counter == null) {
			synchronized (this) {
				if (counter == null) {
					inProgressCntsPerType.put(type, counter = new AtomicInteger());
				}
			}
		}
		return counter;
	}

	protected String getTransferMethod(BaseTask task) {
		return StringUtils.trimToNull(StringUtils.substringAfterLast(task.getType(), "|"));
	}
}
```

2. Rescheduling a task.
   Let's assume, we want to reschedule a task with taskId to be executed in 5 minutes if it is in WAITING state. We need to get the task first to check its status and get current version.
```java
GetTaskResponse task = tasksService.getTask(new GetTaskRequest().setTaskId(taskId));

if (task.getStatus().equals(TaskStatus.WAITING)) {
  tasksService.rescheduleTask(new RescheduleTaskRequest()
          .setTaskId(taskId)
          .setVersion(task.getVersion())
  .setRunAfterTime(ZonedDateTime.now().plusMinutes(5))
  );
};
```
