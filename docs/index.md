# Tw-Tasks-Executor documentation

## Table of Contents
* [Introduction](#intro)
  * [What TwTask is not for](#what-twtask-is-not-for)
  * [Tasks in production, Best Practices](#tasks-in-production-best-practices)
  * [A Task](#a-task)
  * [A Task Handler](#a-task-handler)
  * [Task State Machine](#task-state-machine)
  * [Useful implementations provided by TwTasks using the TwTasks engine](#useful-implementations-provided-by-twtasks-using-the-twtasks-engine)
* [What it's not for](#what-twtask-is-not-for)
* [Infrastructure](infrastructure.md)
* [Usage](usage.md)
* [Examples](examples.md)
* [Algorithms Used](algorithms.md)
* [Rate Limiting](rate-limiting.md)
* [Database performance tests](db-perf-tests.md)
* [Contribution Guide](contributing.md)

## Intro

A framework for executing arbitrary asynchronous code in distributed way with full consistency guarantees.

Basically fire-and-forget-until-alert kind of system.

- Consistency
  - TwTasks guarantees that no set up task will go missing or is processed twice. We don't want to have missing payouts
    or double payouts.
  - TwTasks is able to detect and correctly handle a case, where a same task is tried to set up multiple times, usually because
    of receiving same Kafka message multiple times.
  - TwTask uses optimistic locking for most efficient consistency, even for manual intervention a version has to be always provided.
- Atomicity
  - TwTasks allows to set up tasks inside business logic and guarantees, that only when business logic is successfully
    committed, the set up tasks will be executed. If business logic will be rolled back, the set up tasks will be discarded.
    This will fix the issue we had with Artemis, where in some cases a message was sent before transaction was committed
    or in worse case rolled back.
- Low latency
  - TwTasks can provide sub-millisecond latency, so the task can be executed very quickly (even in other node) after
    business logic commits. Every payout has many asynchronous steps and we can not tolerate any latency when supporting
    instant payouts. The goal was to have exactly the same flow for normal and instant payouts, difference being, that
    instant payouts' tasks have just higher priority.
- Priorities
  - Higher priority tasks will be always run before lower priority tasks. We want instant payouts to have higher priority.
  - TwTasks allows even to stretch the maximum concurrency when all the processing slots are full for high priority tasks.
- Automatic retries
  - When a task fails with an unhandled exception, we can specify if and when it will be retried again. It is very common,
    that during a payout execution the Partner side has technical issues and in that case we would want to retry again after
    1 hour.
- Fault tolerance
  - When a service node or even the whole cluster crashes, we still want to keep our consistency and atomicity guarantees.
  - Not only crashes, but infrastructure problems (Kafka or Database down) are considered and handled.
- Self healing
  - After a crash or even a bug fix, the system is able to resume all the tasks set up, but still maintaining atomicity and
    consistency guarantees.
  - TwTasks has algorithms for in certain situation to safely recover and resume tasks very fast.
- Monitoring and Alerting
  - Sometimes a self healing is impossible (usually because of a bug) or too dangerous. For those cases TwTasks is able
    to notify engineers via logs, Rollbar and/or VictorOps.
  - TwTasks has also NewRelic and XRequestId support built in. It is relatively easy to track chained tasks and how performant
    every task is.
  - JMX and Grafana support is included as well. The engine provides lots of different JMX beans and metrics for its inner state.
  - When ever the engine deals with a specific task, its sets the task id into Slf4j MDC. This can be added into application's log row.
- Manual or automatic intervention
  - There is a need and possibility to stop a task, resume a task, retry a task or mark it as completely failed and thus turn off further alerts.
  - TwTasks provides programmatic and REST API for that. There is a Ninjas2 interface as well, based on that REST API.
- Sophisticated concurrency control
  - In order to protect the service from trashing because of too many concurrent threads/processes, TwTasks allows engineer
    to set up proper concurrency limits (lets say maximum of 10 processes in one node at one time). The concurrency control setup
    is very simple, but at the same time allows extremely complex scenarios, where for example we want to have 30 total processes,
    15 total payout execution, but not more than 5 per partner.
  - TwTasks is also solving the fundamental problem with all other queue based task executors - one slow task type, can not hinder
    the processing of other task types. Let's say, one of our partners is very slow and Ops just submitted 10000 of that partner Payouts.
    Just after those 10000 other Ops submitted 10 payouts for another partner. TwTasks will correctly understand in that case,
    that it can maybe execute maximum of 5 payouts for 1st partner, but in parallel already execute 2nd partner payouts as well.
    Other task executors would have to wait for 10000 1st partner payouts, before 2nd partner can even have chance.
  - Concurrency control is fully programmatic, one can easily write an implementation where we can even stretch the processing
    power for high priority tasks.
- Sharding
  - TwTasks provides even sharding, where some type of tasks are executed completely parallel from other tasks. It is useful
    to completely separate some components of service, for example slower payout flow and faster sending out emails or messages. The shards
    in TwTasks are called processing buckets.
- Graceful Shutdown
  - Even when TwTasks is crash tolerant, in usual cases we would like to keep the log and alerting spam minimal. TwTasks
    handles service shutdown, but also startup gracefully.
- Scheduled Tasks
  - When setting up a task, an engineer can specify, when a particular task should run - immediately or lets say after 1 hour.
    For payouts, it is very common, that some payouts execution (especially on retries) is desired to not run immediately, but
    at very specific time.
- Good Performance
  - Lots of effort is put into performance and efficiency. Local machine performance tests show about 1750 tasks set up and executed in a second.
    During that, CPU utilization is low and the bottleneck is underlying database, especially its commit (read fsync) speed.
  - 1750 tasks/s should always considered when designing algorithms based on TwTasks, for example its much more efficient to send
    batch of Kafka messages with one task, instead creating a separate task for every single message.
- Support for both synchronous and asynchronous tasks.
  - Payout execution for example is synchronous, but sending a Kafka message is asynchronous.
- Support for both Postgres and MySQL databases.
  - TransferWise uses both databases in different services.
- Cluster support
  - A task can be executed in any node having free processing power first. It is very common (actually 1/(nodes count) chance),
    that a task is set up by one node, but execution is happening on another node.
  - It is guaranteed, that even in a cluster, a task processing happens only in one node and only once.
  - Depending on a cluster size there are different triggering algorithms balancing latency, ordering and resource efficiency.
  - Self healing and task scheduling maintenance processes run only in one node (leader election) to maximize efficiency.

Design of TwTasks has avoided following practices and approaches.
- Triggering tasks by database polling (QueueEvent, Outgoing Messages)
  Mainly because of latency and performance reasons. You can not have 1 ms latency with database polling approach.
  Database polling is not used for usual tasks and happy flows, however it is still used for scheduled tasks (notice, that a retry is also handled
  with a task scheduling), self healing and monitoring needs. However the polling has long interval and only one node in the cluster
  does it. In the future a better algorithm could be designed for scheduled tasks.


## Tasks in production, Best Practices
Always resolve tasks in ERROR state ASAP (mark as FAILED or for reprocessing).
Only in that case there will be any value in Rollbar alerts and VictorOps incidents.
If you have functionality that tends to create FAILED tasks but is not critical to complete, just make the handler not fail on the last retry.

Tasks that are still `WAITING` for next retry (not in `ERROR` state yet) can be triggered sooner than it's `nextEventTime` by using _Resume_ feature in Ninjas2.

## A Task
The central piece of TwTasks is of course a task.

A task is essentially a record in the database, with following important attributes.

- Type
  A unique identifier of a type of task. A task handler and thus everything else is found by this type.
  For example `SEND_EMAIL`. A type can sometimes contain other information to help concurrency policy or processing policy.
  For example, in case of `DELIVER_BATCH|PARTNERX`, Payout Service's Concurrency Policy can apply a special rules for PARTNERX partner.
- Data
  Task payload. Huge payloads are fully supported, however for performance reasons it is recommended to not use them.
  Even when there is a need for huge payload, the actual payload could be kept somewhere else and only reference to it is
  added into Data field.
- NextEventTime
  Indicates when something should happen with the task, depending on a context.
  Could be when stuck task detector is kicked in, but also when a task needs to be executed (as a scheduled task).
  We use one field for multiple contexts, to have extremely efficient and small index capable supporting all cases.
- ProcessingTriesCount
  How many times the task has been tried to processed. This is commonly used to decide if and when a next retry should happen.
- Priority
  In default configuration 0-9, with default being 5.
- Status
- Id
  Unique UUID either autogenerated or provided by the application.
- Subtype
  Just for clarity, not used by the engine. For example, in Kafka Messages sending task it could be the destination topic.

A task can be added and scheduled with `com.transferwise.tasks.ITasksService.addTask`, further documentation can be found in its JavaDoc.

## A Task Handler
A task processing is driven by a task handler, which supports the engine with task processor, retry policy, concurrency policy,
processing policy and retry policy.

![alt Task and Handlers](TwTasksHandlers.png)

One task handler can support multiple task types. Also any processor or retry policy can support multiple task handlers and so
indirectly multiple task types as well.

In many cases a TwTasks shard has only one central Concurrency Policy, to control the all concurrency from single place
(e.g. if maximum number of 30 threads has to be forced).

A task processor is always provided by an engineer, whereas all the policies have one or many default implementation, e.g. `ExponentialTaskRetryPolicy`.

## Task State Machine
![alt State Machine](TwTasksStates.png)

## Useful implementations provided by TwTasks using the TwTasks engine
1. Consistently sending Kafka Messages
   Can be done using `com.transferwise.tasks.impl.tokafka.IToKafkaSenderService`. Consult its JavaDoc for usage.
   Using this you get same guarantees, monitoring and alerting as for any other task.

2. Cluster wide periodic jobs.
   Usually this is done by Quartz, but Quartz has many problems. It is too heavyweight and it actually can still execute same job
   in multiple nodes at the same time, so it does not give you any guarantees, even when configured "correctly".

The trick with TwTasks is to provide a RetryPolicy, which never stops Retrying.

```java
@Component
public class SampleCronRegistrar {
    // The UUID is generated by engineer and hardcoded. This can also be used later for any kind of manual intervention.
	private static final UUID UUID_SAMPLE_CRON = UUID.fromString("5c60b661-6d2b-43ac-974e-434512f776a0");

	@Autowired
	private ITasksService tasksService;

	@PostConstruct
	public void init() {
		/*
		 * This is fast no-OP when task with same UUID is already present.
		 * We set runAfterTime value, so all nodes will have time to get SampleCronTaskHandler released.
		 * Otherwise there is a chance that the task goes immediately to ERROR state, because the node chosen for execution
		 * does not have the handler released yet.
		 */
		tasksService.addTask(new ITasksService.AddTaskRequest()
				.setTaskId(UUID_SAMPLE_CRON).setType(TT_SAMPLE_CRON).setRunAfterTime(ZonedDateTime.now().plusHours(1)));
	}
}
```

```java
@Component
@Slf4j
public class SampleCronTaskHandler extends TaskHandlerAdapter {
	public static final String TT_SAMPLE_CRON = "SAMPLE_CRON";

	public SampleCronTaskHandler() {
		super(s -> s.equals(TT_SAMPLE_CRON), (ISyncTaskProcessor) task -> {
			// Do the work
			log.info("Alive and kicking!");
			// We could throw an exception for a retry, but TwTasks also provide special, spam(logs) free, return code.
			return new ISyncTaskProcessor.ProcessResult().setResultCode(ISyncTaskProcessor.ProcessResult.ResultCode.COMMIT_AND_RETRY);
		});
		setRetryPolicy(new ITaskRetryPolicy() {
			@Override
			public ZonedDateTime getRetryTime(ITask task, Throwable t) {
				return ZonedDateTime.now().plusMinutes(45); // We could also use CronUtils here, if we would want to use cron expressions.
			}
			@Override
			public boolean resetTriesCountOnSuccess(IBaseTask task) {
				return true; // Don't increase the task tries count when job was correctly processed.
			}
		});
	}
}
```

3. Listening Kafka Messages
   You have to provide an implementation Bean for `IKafkaMessageHandler` and that's it. You get
- Fallback sites support.
- Automatic consistent commits.
- Spamless(logs) failover and re-balancing.

4. Translating Kafka Messages into Tasks.
   You have to provide an implementation Bean for `KafkaMessageToTaskConverter` and that's it.

```java
@Bean
public KafkaMessageToTaskConverter processConfirmationsMessageToTaskConverter() {
    return new KafkaMessageToTaskConverter<>(ProcessConfirmationsCommand.class, (event, cr, tr) -> {
        // If you return false, the message will be ignored, no task created. This can be used to filter out messages.
        tr.setTaskId(event.getUuid()) // We use event's UUID for deduplication. Notice that if task already exists with that UUID, nothing is done.
                .setDataString(cr.value())
                .setType("PROCESS_CONFIRMATIONS")
                .setSubType(event.getTransferMethod());
        return true;
    }, new IKafkaMessageHandler.Topic().setAddress("PROCESS_CONFIRMATIONS_COMMANDS").setSuggestedPartitionsCount(4));
}
```

If you have more than one message processor *for the same topic* in same service, you can create a variation of message uuid:
```
.setTaskId(new UUID(event.uuid.getMostSignificantBits(), event.uuid.getLeastSignificantBits() + 1))

```

## What TwTask is not for
Notice, that TwTask is for inner (micro-)service use. If you want to set up an asynchronous process in another service, you should
send a Kafka Message to that service instead and this service could set up a TwTask itself on receiving the message.