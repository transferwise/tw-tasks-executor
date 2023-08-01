# ChangeLog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres
to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

#### 1.40.3 - 2023/08/01

### Added

* Support for Spring Boot 3.1

### Bumped

* Build against Spring Boot 3.0.6 --> 3.0.7
* Build against Spring Boot 2.7.11 --> 2.7.13
* Build against Spring Boot 2.6.14 --> 2.6.15

#### 1.40.2 - 2023/07/14

### Added

* introduced a new configuration parameter `tw-tasks.core.no-op-task-types` that allows a default no operation task handler to pick up deprecated task types in your service.

#### 1.40.1 - 2023/07/12

### Fixed

* `commitSync` operation sometimes reporting a WakeupException.

#### 1.40.0 - 2023/06/12

### Added
* CronJob annotation for Spring bean's methods

#### 1.39.2 - 2023/06/06

### Fixed

* Circular dependency with graceful shutdown.

* docker-compose on linux.

#### 1.39.1 - 2023/04/19

### Changed
* Kafka consumer offset duration is always considered as positive since we cannot reset the offsets to future timestamps.
* Both `PT1H` and `-PT1H` are treated the same ie `PT1H`. This value gets subtracted by now() timestamp.
* Added second kafka consumer for the tests in `SeekToDurationOnRebalanceListenerIntTest` class
* Updated the `docker-compose.yml` to make kafka container run as expected.

## 1.39.0 - 2023/05/03

### Added

* Support for Spring Boot 3.0.

### Changed

- Replaced `@Validated` annotation with custom call to validator.
  `@Validated` annotation based approach made services startup slow.
- Improved the graceful shutdown speed to be less than medium delay interval (by default 5s).
- Changed `MySqlTaskDao` to `JdbcTaskDao`, because some Postgres users got confused/spooked having "mysql" in their stack trace.

### Removed

* Support for Spring Boot 2.5.

## 1.38.0 - 2023/01/17

### Changed

* Added IPartitionKeyStrategy interface. This interface allows for custom strategies to be implemented by clients
  that want more control over the partition key generation.

* Add a basic implementation to IPartitionKeyStrategy: RandomPartitionKeyStrategy. This strategy always generates a
  random partition key (like the previous behaviour).

* Included IPartitionKeyStrategy into SimpleTaskProcessingPolicy.

## 1.37.1 - 2022/11/28

### Changed

* The Spring Boot Version from which the library dependencies are derived, was moved from 2.7 to 2.6.
  This should give better compatibility, as backward compatibility is usually better than forward one.

## 1.37.0 - 2022/11/17

### Changed

* Tasks' triggers' offset is committed synchronously, when partitions are revoked.

* Reworked paranoid tasks cleaner to work with latest mariadb drivers.

* Made it compatible with Spring Boot 2.7

* Removing support for Spring Boot 2.4

### Removed

* Metric `kafkaTasksExecutionTriggerer.failedCommitsCount` was removed.
  `kafkaTasksExecutionTriggerer.commitsCount` got `sync` and `success` tags.

## 1.36.0 - 2022/10/24

### Added

* Some initialization logs allowing to understand which lock keys are used.

### Changed

* `ConsistentKafkaConsumer` is asynchronously commiting offsets now with an interval, by default once in 5 seconds per partition.
  Notice that tw-tasks-kafka-listener is deprecated.
* `ConsistentKafkaConsumer` is doing a synchronous commit, during revoking of partitions.
  This would make it much less likely that a node getting those partitions assigned will find duplicates.

### Fixed

* Inserting unique key into database is more consistent.

## 1.35.0 - 2022/05/12

### Changed

* Using `CooperativeStickyAssignor,RangeAssignor` when it is detected that `kafka-clients` is `3.+`.
* Task grabbing is using just implicit transactions.

## 1.34.0 - 2022/04/22

### Added

* Simple and small `tw-tasks-jobs-test-spring-boot-starter` module to reduce a bit of boilerplate in services testing jobs.

### Changed

* Using `CooperativeStickyAssignor` when it is detected that `kafka-clients` is `3.+`.

## 1.33.1 - 2022/04/21

### Fixed

* Putting back `ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() + "," + RangeAssignor.class.getName()`
  for Kafka consumers. Typically, the tw-tasks consumer group is shared with other kafka consumers in a service, so just `CooperativeStickyAssignor`
  would create issues on older kafka-clients.

## 1.33.0 - 2022/04/05

### Fixed

* Deadline is removed after task processing, allowing follow-up database operations to succeed in any case.
  Example case: task processing threw `DeadlineExceededException` and asking retry time threw it again.

## 1.32.1 - 2021/01/05

### Fixed

- Spring's 4.x `TransactionSynchronization` does not have default methods implemented.
  In order for tw-tasks to work on Spring 4.x, restored `TransactionSynchronizationAdapter` class.

## 1.32.0 - 2021/01/03

### Changed

- `tw-tasks-kafka-listener` module is not depending on `spring-kafka` anymore, so it can be used also on older services.
- On offset loss in `tw-tasks-kafka-listener`, by default, we are rewinding back to 1 hours.
- Reduced integration tests suite runtime from approximately 2 minutes to 25 seconds. This was mainly achieved to have different Kafka consumer groups
  for different things/tests and thus avoid lengthy stop-the-consume re-balancing pauses. Can be reduced a bit more, but I had this work time-boxed.
- All Kafka consumers and producers register micrometer metrics.
- Small tweaks to consumers and producers configs. Important one
  is `ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName() + "," + RangeAssignor.class.getName()`
- `tw-tasks-kafka-listener` now rewinds 1 hour, when offset is lost.
  This can be changed via `tw-tasks.impl.kafka.listener.autoResetOffsetTo` property.

## 1.31.0 - 2021/12/29

### Removed

- Removed deprecated `coreKafkaListenerTopicsConfiguringEnabled` configuration property.

### Changed

- Migrated CI from Circle to GHA.

## 1.30.1 - 2021/12/09

- Stuck tasks resumer was hanging due to semaphore not getting released.
- Stuck tasks count metric now also has task status dimension.

## 1.30.0 - 2021/12/07

- Scheduled and stuck tasks are now resumed concurrently, by default with the parallelism of 10. This eliminates a bottleneck for services relying on
  large volume of scheduled tasks.

## 1.29.0 - 2021/05/31

- JDK 11+ is a requirement.
- Opensource facelift.

## 1.28.0 - 2021/05/26

- Better support for implementing rate-limiting as an `ITaskConcurrencyPolicy` implementation.

## 1.27.1 - 2021/05/10

- Checking of some database transactions state is done only when assertions are enabled.

## 1.27.0 - 2021/04/08

- We don't check if task is in submitted status, when grabbing, by default. The version check is enough. It can be turned on though via a property, it
  can be useful for tw-task test suites.

## 1.26.0 - 2021/03/14

- Refactoring and optimizing code around metrics.
- Fixed high CPU usage around `TasksProperties`, due to `@Validated` annotations.
- Various small optimizations and library upgrades.

## 1.25.0 - 2021/03/10

- Support type-level task management configuration `tw-tasks.core.tasks-management.type-specific`

## 1.24.0 - 2021/02/18

- Core and tasks triggering system does not depend on Spring Kafka, nor it's configuration. Services have to now
  specify `tw-tasks.core.triggering.kafka.bootstrap-servers` parameter.
- Tasks triggering system has its own ObjectMapper instance.

## 1.23.0 - 2021/02/17

- Node's tasks are resumed on startup by the same logic we resume other stuck tasks. On the startup, the current node tasks in `PROCESSING` state will
  be marked to `ERROR` now. This is a safer default option. For example, when `nodeId` is wrongly configured and not unique around the whole service
  cluster, we can easily have already executing task getting wrongly resumed and having it being executed twice at the same time.
- Fixed also start-up race conditions around same-node `PROCESSING` tasks resuming. It was possible to start processing a task and this same task
  getting immediately resumed by the start-up logic in the `TasksResumer` component.

## 1.22.3 - 2021/01/15

- Increases the tasks grabbing maximum concurrency from 10 to 25 and makes it configurable by a property.
- Fixes Base64 encoder package.

## 1.22.2 - 2021/01/15

- Fixes of fetching task info and data via management endpoints for tasks with empty data.
- Allows fetching task data as Base64.

## 1.22.1 - 2021/01/06

- Fixes approximate tables rows counts queries.

## 1.22.0 - 2021/01/06

- Removes `copyDataToTwTaskField` property and sets `1.21.1` as minimum upgradable version. We don't write into old `data` field anymore.

## 1.21.1 - 2021/01/03

- Task data is still saved into old data field, to allow seamless upgrade process. When upgrade from versions before 1.21.1 is finished, i.e. all
  cluster nodes have the new version;
  `TasksProperties.copyDataToTwTaskField` can be set to `false`, stopping writing into the old data field.
- `TasksProperties.Environment.previousVersion` is mandatory.

## 1.21.0 - 2020/12/21

- Task data is now binary.
- The payload is kept in the storage in compressed format. By default, gzip is used.
- Introduced tw-tasks-core-test-spring-boot-starter for simpler test setup in services.

Migration for MySql.

```mariadb
CREATE TABLE tw_task_data
(
  task_id     BINARY(16) PRIMARY KEY NOT NULL,
  data_format INT                    NOT NULL,
  data        LONGBLOB               NOT NULL
);
```

```postgresql
CREATE TABLE tw_task_data
(
  task_id     UUID PRIMARY KEY NOT NULL,
  data_format INT              NOT NULL,
  data        BYTEA            NOT NULL
) WITH (toast_tuple_target = 8160);

ALTER TABLE tw_task_data
  ALTER COLUMN data SET STORAGE EXTERNAL;
```

## 1.20.0 - 2020/12/20

- Removed deprecated kafka-publisher modules. Tw-tkms has been successfully used in 19 services and is stable now.

## 1.19.3 - 2020/12/18

- Stuck tasks warning has information and metrics about specific task types.

## 1.19.2 - 2020/11/11

- Remove AdminClientTopicPartitionsManager and remove configureKafkaTopics. You need to remove the configuration
  property: `tw-tasks.core.configure-kafka-topics`.

## 1.19.1 - 2020/11/10

- Fix AdminClient Jmx registration issue.

## 1.19.0 - 2020/11/01

- Allowing most beans defined by auto configuration to be overridden.

## 1.18.0 - 2020/11/01

- MDC corrections. Following MDC keys are now set for tasks under processing:

* `twTaskId`
* `twTaskVersion`
* `twTaskType`
* `twTaskSubType`

`twTaskVersionId` is not set anymore.

- Task can now define its TwContext criticality and owner.

- Lots of corrections around entry points creation.

## 1.17.0 - 2020/10/31

- Optimization and configuration for fetching approximate tasks and unique keys count by cluster wide tasks state monitor. Consult
  with `com.transferwise.tasks.TasksProperties.ClusterWideTasksStateMonitor` for added configuration options.

- Minor external libraries upgrades.
- Minor testsuite optimizations.

Some transactions are now using isolation level READ_UNCOMMITTED. If you are using JTA transaction manager, you may have to do two things.

1. Wrap your datasource into `org.springframework.jdbc.datasource.IsolationLevelDataSourceAdapter`
2. Set `org.springframework.transaction.jta.JtaTransactionManager.setAllowCustomIsolationLevels` to true.

## 1.16.0 - 2020/10/28

Use separate DAOs for Core/Test/Management.

- ITaskDao - data access operations used by the core and extensions.
- IManagementTaskDao - data access operations used by the management extension.
- ITestTaskDao - data access operations used for testing purposes

Users of `tw-tasks-core-test` need to configure `ITestTaskDao` in the test configuration as from this version it is required by `TestTasksService`.

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

## 1.15.1 - 2020/20/16

- Partitions manager will log a warn only when a topic is missing or configured number of partitions is different from existing ones.

## 1.15.0 - 2020/10/14

- Switched away from testcontainers, used docker-compose plugin for all integration tests.
- Removed support for xRequestId.

## 1.14.2 - 2020/09/27

- Minor bugfixes for approximate tasks count in the database, related to multi schema setups.
- MySQL INSERT IGNORE has additional checks to make sure the failure was about duplicate records and not about something else.

## 1.14.1 - 2020/09/21

- Added metrics for knowing approximate tasks count in the database.

## 1.14.0 - 2020/09/16

- We are starting to use sequential UUIDs, which are more suitable for database storage. Gains are especially large and exponential on MariaDb. 1mln
  tasks 2x speed on db perf test. 2mln tasks 4x speed on db perf test.

Technically we use 38 bit timestamp (millis) prefix on random UUID as implicit task ids.

https://www.informit.com/articles/article.aspx?p=25862
https://www.2ndquadrant.com/en/blog/sequential-uuid-generators/
https://en.wikipedia.org/wiki/Universally_unique_identifier#As_database_keys

- (id,version) index was removed on Postgres as well, making db perf test to run 25% faster.

- MariaDb schema for new services was redesigned. However, the code is still working and keeps working with older schema as well.

- Another, more optimal table schema was tested and proposed for MariaDb applications which for whatever reasons are forced to use random UUIDs with
  large number of tw tasks.

- Added a db perf test to `demoapp` and `DemoAppRealTest`, which is more suitable to compare database bottlenecks tests.

- When a task is being set to a final state, the next_event_time is set to current time. This will make the task cleaning process more accurate.

## 1.13.0 - 2020/09/10

- Old tasks are now cleaned by ids only and not checking their versions. It allows to execute multivalue queries, which should be more efficient.
  Previous situation can be set by `TasksProperties.paranoidTasksCleaning=true`.

## 1.12.0 - 2020/08/31

- Moving away from deprecated LeaderSelector to LeaderSelectorV2.
- Added new metric `twTasks.task.addings.count` for tracking adding of new tasks.
- Background jobs start and stop messages contain `group.id`. It allows quickly to understand, if some service is using another service's identifier.
- Upgraded external libraries to latest.

## 1.11.0 - 2020/08/27

- Optimized a TasksResumer query executed on startup for Postgres. Postgres was likely to decide to not use `(status, next_event_time)` and do a full
  scan instead.
- Properties `minPriority` and `maxPriority` on `tw-tasks.core` were renamed to `highestPriority` and `lowestPriority`. It will hopefully make it more
  clear, that lower priority numbers mean higher chance to be executed first.

## 1.10.1 - 2020/08/18

- Fixes a bug, where using a max priority for a task causes a null pointer exception.

## 1.10.0 - 2020/08/13

- IKafkaMessageHandler Topics can now specify a shard. Every shard will have it's own KafkaConsumer and processing thread. It is useful in scenarios
  where low latency processing is desired for a specific topic. The downside of multiple shards is having more KafkaConsumers per application,
  possibly increasing the load on Kafka server.
- tw-leader-selector was upgraded, it now brings in tw-curator. This in turn means, that you don't have to define a CuratorFramework bean in your
  application, it will be created automatically if missing.

## 1.9.0 - 2020/07/10

- Optimized some queries for a case where there is enormous number of waiting or stuck tasks.

## 1.8.3 - 2020/07/10

- Debug metrics are disabled by default.

## 1.8.2 - 2020/07/09

- We are marking all buckets as dirty, when some concurrency slot frees up. To support cases where multiple buckets have the same concurrency policy.

## 1.8.1 - 2020/07/08

- Added some debug metrics for tasks processing cycle.

## 1.8.0 - 2020/06/30

- Removed 1.7.5 and 1.7.4 version from repositories and correctly increased the minor version instead. Because the ClockHolder change may need some
  minor changes in services test suites.

## 1.7.5 - 2020/06/30

- Moving away from global ClockHolder to mock the time in tests. In that way we will create less surprises and flakiness for services also needing to
  mock that global time for other reasons.

## 1.7.4 - 2020/06/29

- Reducing jobs logs spam in applications test suite.

## 1.7.2 - 2020/05/22

- TaskHandlerRegistry is initializing handlers list in lazy way to avoid possible circular dependencies in applications.

## 1.7.1 - 2020/05/21

- ITestTaskService got 2 new methods for controlling the automatic tasks processing:
  - `stopProcessing()`
  - `resumeProcessing()`

Fixing possible race-condition in `ClusterWideTasksStateMonitor`.

## 1.7.0 - 2020/05/14

Moved https://github.com/transferwise/tw-tasks-jobs to the main repository in a form of extension that consists of extension core, spring boot starter
and test components. The typical tw-tasks-jobs library consumer will replace tw-tasks-jobs dependency with:

```
implementation("com.transferwise.tasks:tw-tasks-jobs-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-jobs-test:${twTasksVersion}")
```

Note that `com.transferwise.tasks.impl.jobs.JobsAutonfiguration` is replaced with
`com.transferwise.tasks.ext.jobs.autoconfigure.TwTasksExtJobsAutoConfiguration`

## 1.6.0 - 2020/05/14

The project is split on modules. The tw-tasks-executor artifact is no longer published. From now on there is a core module and related extensions that
can be easily switched on and off. The typical library consumer will replace tw-tasks-executor dependency with:

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

## 1.5.0 - 2020/04/20

ExponentialTaskRetryPolicy is now handling arithmetic overflows. But for that, the multiplier was refactored from double to integer.

## 1.4.0 - 2020/04/20

TwTasksManagement API has a getTask endpoint.

You can now secure all tasks management endpoints by specifying
`TasksProperties.TasksManagement.viewTaskDataRoles`