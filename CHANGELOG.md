#ChangeLog

Describes notable changes.

#### 1.20.0 - 2020/12/20
- Removed deprecated kafka-publisher modules.
Tw-tkms has been successfully used in 19 services and is stable now.

#### 1.19.3 - 2020/12/18
- Stuck tasks warning has information and metrics about specific task types.

#### 1.19.2 - 2020/11/11
- Remove AdminClientTopicPartitionsManager and remove configureKafkaTopics.
  You need to remove the configuration property: `tw-tasks.core.configure-kafka-topics`.
#### 1.19.1 - 2020/11/10
- Fix AdminClient Jmx registration issue.

#### 1.19.0 - 2020/11/01
- Allowing most beans defined by auto configuration to be overridden.

#### 1.18.0 - 2020/11/01
- MDC corrections.
Following MDC keys are now set for tasks under processing:
* `twTaskId`
* `twTaskVersion`
* `twTaskType`
* `twTaskSubType`

`twTaskVersionId` is not set anymore.

- Task can now define its TwContext criticality and owner.

- Lots of corrections around entry points creation.

#### 1.17.0 - 2020/10/31
- Optimization and configuration for fetching approximate tasks and unique keys count by cluster wide tasks state monitor.
Consult with `com.transferwise.tasks.TasksProperties.ClusterWideTasksStateMonitor` for added configuration options.

- Minor external libraries upgrades.
- Minor testsuite optimizations.

Some transactions are now using isolation level READ_UNCOMMITTED. If you are using JTA transaction manager, you may have to do
two things.
1. Wrap your datasource into `org.springframework.jdbc.datasource.IsolationLevelDataSourceAdapter`
2. Set `org.springframework.transaction.jta.JtaTransactionManager.setAllowCustomIsolationLevels` to true.

#### 1.16.0 - 2020/10/28
Use separate DAOs for Core/Test/Management.
- ITaskDao - data access operations used by the core and extensions.
- IManagementTaskDao - data access operations used by the management extension.
- ITestTaskDao - data access operations used for testing purposes

Users of `tw-tasks-core-test` need to configure `ITestTaskDao` in the test configuration
as from this version it is required by `TestTasksService`.

```
// either
@Bean
public ITestTaskDao postgresTestTaskDao(DataSource dataSource, TasksProperties tasksProperties) {
  return new PostgresTestTaskDao(dataSource, tasksProperties);
}

// or
@Bean
public ITestTaskDao mysqlTestTaskDao(DataSource dataSource, TasksProperties tasksProperties) {
  return new MySqlTestTaskDao(dataSource, tasksProperties);
}
```

#### 1.15.1 - 2020/20/16
- Partitions manager will log a warn only when a topic is missing or configured number of partitions is different from existing ones.

#### 1.15.0 - 2020/10/14
- Switched away from testcontainers, used docker-compose plugin for all integration tests.
- Removed support for xRequestId.

#### 1.14.2 - 2020/09/27
- Minor bugfixes for approximate tasks count in the database, related to multi schema setups.
- MySQL INSERT IGNORE has additional checks to make sure the failure was about duplicate records and not about something else.

#### 1.14.1 - 2020/09/21
- Added metrics for knowing approximate tasks count in the database.

#### 1.14.0 - 2020/09/16
- We are starting to use sequential UUIDs, which are more suitable for database storage.
Gains are especially large and exponential on MariaDb.
1mln tasks 2x speed on db perf test.
2mln tasks 4x speed on db perf test.

Technically we use 38 bit timestamp (millis) prefix on random UUID as implicit task ids.

https://www.informit.com/articles/article.aspx?p=25862
https://www.2ndquadrant.com/en/blog/sequential-uuid-generators/
https://en.wikipedia.org/wiki/Universally_unique_identifier#As_database_keys

- (id,version) index was removed on Postgres as well, making db perf test to run 25% faster.

- MariaDb schema for new services was redesigned.
However, the code is still working and keeps working with older schema as well.

- Another, more optimal table schema was tested and proposed for MariaDb applications which for whatever reasons are forced
to use random UUIDs with large number of tw tasks.

- Added a db perf test to `demoapp` and `DemoAppRealTest`, which is more suitable to compare database bottlenecks tests.

- When a task is being set to a final state, the next_event_time is set to current time.
This will make the task cleaning process more accurate.

#### 1.13.0 - 2020/09/10
- Old tasks are now cleaned by ids only and not checking their versions. It allows to execute multivalue queries, which should be more efficient.
Previous situation can be set by `TasksProperties.paranoidTasksCleaning=true`.

#### 1.12.0 - 2020/08/31
- Moving away from deprecated LeaderSelector to LeaderSelectorV2.
- Added new metric `twTasks.task.addings.count` for tracking adding of new tasks.
- Background jobs start and stop messages contain `group.id`.
It allows quickly to understand, if some service is using another service's identifier.
- Upgraded external libraries to latest.

#### 1.11.0 - 2020/08/27
- Optimized a TasksResumer query executed on startup for Postgres.
Postgres was likely to decide to not use `(status, next_event_time)` and do a full scan instead.
- Properties `minPriority` and `maxPriority` on `tw-tasks.core` were renamed to `highestPriority` and `lowestPriority`.
It will hopefully make it more clear, that lower priority numbers mean higher chance to be executed first.

#### 1.10.1 - 2020/08/18
- Fixes a bug, where using a max priority for a task causes a null pointer exception.

#### 1.10.0 - 2020/08/13
- IKafkaMessageHandler Topics can now specify a shard. Every shard will have it's own KafkaConsumer and processing thread.
It is useful in scenarios where low latency processing is desired for a specific topic.
The downside of multiple shards is having more KafkaConsumers per application, possibly increasing the load on Kafka server.
- tw-leader-selector was upgraded, it now brings in tw-curator.
This in turn means, that you don't have to define a CuratorFramework bean in your application, it will be created
automatically if missing.

#### 1.9.0 - 2020/07/10
- Optimized some queries for a case where there is enormous number of waiting or stuck tasks.

#### 1.8.3 - 2020/07/10
- Debug metrics are disabled by default.

#### 1.8.2 - 2020/07/09
- We are marking all buckets as dirty, when some concurrency slot frees up.
To support cases where multiple buckets have the same concurrency policy.

#### 1.8.1 - 2020/07/08
- Added some debug metrics for tasks processing cycle.

#### 1.8.0 - 2020/06/30
- Removed 1.7.5 and 1.7.4 version from repositories and correctly increased the minor version instead.
Because the ClockHolder change may need some minor changes in services test suites.

#### 1.7.5 - 2020/06/30
- Moving away from global ClockHolder to mock the time in tests.
In that way we will create less surprises and flakiness for services also needing to mock that global time
for other reasons.

#### 1.7.4 - 2020/06/29
- Reducing jobs logs spam in applications test suite.

#### 1.7.2 - 2020/05/22
- TaskHandlerRegistry is initializing handlers list in lazy way to avoid possible circular dependencies in applications.

#### 1.7.1 - 2020/05/21
- ITestTaskService got 2 new methods for controlling the automatic tasks processing:
  - `stopProcessing()`
  - `resumeProcessing()`

Fixing possible race-condition in `ClusterWideTasksStateMonitor`.

#### 1.7.0 - 2020/05/14
Moved https://github.com/transferwise/tw-tasks-jobs to the main repository in a form
of extension that consists of extension core, spring boot starter and test components.
The typical tw-tasks-jobs library consumer will replace tw-tasks-jobs dependency with:

```
implementation("com.transferwise.tasks:tw-tasks-jobs-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-jobs-test:${twTasksVersion}")
```
Note that `com.transferwise.tasks.impl.jobs.JobsAutonfiguration` is replaced with
`com.transferwise.tasks.ext.jobs.autoconfigure.TwTasksExtJobsAutoConfiguration`

#### 1.6.0 - 2020/05/14
The project is split on modules. The tw-tasks-executor artifact is no longer published.
From now on there is a core module and related extensions that can be easily switched on and off.
The typical library consumer will replace tw-tasks-executor dependency with:

```
implementation("com.transferwise.tasks:tw-tasks-core-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-incidents-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-kafka-listener-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-management-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-core-test:${twTasksVersion}")
```

Note that _tw-tasks-incidents_ and _tw-tasks-kafka-listener_ are deprecated and soon will be removed
- Build alerting based on exposed metrics instead of using _tw-tasks-incidents_
- Use spring-kafka or another kafka library instead of using _tw-tasks-kafka-listener_

#### 1.5.0 - 2020/04/20
ExponentialTaskRetryPolicy is now handling arithmetic overflows.
But for that, the multiplier was refactored from double to integer.

#### 1.4.0 - 2020/04/20
TwTasksManagement API has a getTask endpoint.

You can now secure all tasks management endpoints by specifying
`TasksProperties.TasksManagement.viewTaskDataRoles`
