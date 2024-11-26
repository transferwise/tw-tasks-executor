# Usage

##### Add dependencies
```groovy
implementation("com.transferwise.tasks:tw-tasks-core-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-kafka-listener-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-management-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-core-test:${twTasksVersion}")
```

##### Create database tables
- [MySQL/MariaDB/Percona](https://github.com/transferwise/tw-tasks-executor/blob/master/tw-tasks-core/src/main/resources/db/changelog/db.tw-tasks-mysql.xml)
- [Postgres](https://github.com/transferwise/tw-tasks-executor/blob/master/tw-tasks-core/src/main/resources/db/changelog/db.tw-tasks-postgres.xml)

##### Add at least following configuration options
```yml
tw-tasks:
  core:
    group-id: MyFancyServiceName # Use only symbols suitable for Kafka topic!
    db-type: MYSQL
    zookeeperConnectString: ${VALUE_HERE}
    triggering:
      kafka:
        bootstrapServers: ${VALUE_HERE}
    environment:
      previousVersion: "1.37.1" # put current version if you just added the lib

```

##### Create Kafka topic
(If your server does not support auto topic creation)

`twTasks.MyFancyServiceName.executeTask.default`
- partitions count should be usually the service nodes count + canaries count.

##### Configure Zookeeper access by providing CuratorFramework bean.

- [*Recommended way of setting up curator*](https://github.com/transferwise/tw-curator)

- Manual way of setting up curator:
```java
@Bean(destroyMethod = "close")
public CuratorFramework curatorFramework() {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString(connectString).build()
    curatorFramework.start();
    return curatorFramework;
}
```

##### Configure Kafka access by configuring Spring-kafka module
```yaml
spring:
  kafka:
    bootstrap-servers: host.local:9092
    client-id: MyFancyServiceName
    consumer:
      groupId: 'payout-service'
      enableAutoCommit: false
```

##### Execute Your First Task
First, provide a task handler.
```java
@Bean
public ITaskHandler helloWorldTaskHandler() {
    return new TaskHandlerAdapter(task -> task.getType().startsWith("HELLO_WORLD_TASK"), (ISyncTaskProcessor) task -> System.out.println("Hello World!"))
        .setConcurrencyPolicy(taskConcurrencyPolicy())
        .setProcessingPolicy(new SimpleTaskProcessingPolicy().setMaxProcessingDuration(Duration.ofMinutes(30)))
        .setRetryPolicy(new ExponentialTaskRetryPolicy().setDelay(Duration.ofSeconds(5)).setMultiplier(2).setMaxCount(20).setMaxDelay(Duration.ofMinutes(120)));
}
```
Register a Task Execution.
```java
tasksService.addTask(new ITasksService.AddTaskRequest().setType("HELLO_WORLD_TASK"));
```

Write a port-level integration spec.

Profit!

##### In Production you want to enable alerts

In your staging and/or production kubernetes manifests add the global alert:

Just replace the "YOUR_*" placeholders below.

```yaml
prometheus:
  enabled: true
  globalAlerts:
    tooManyTwTasksInErrorState:
      name: tooManyTwTasksInErrorState
      summary: Number of TwTasks in ERROR state.
      description: Task has {{ $value }} errors. There must be 0 tasks in ERROR state. Investigate the problem and retry or mark as FAILED here - https://ninjas.transferwise.com/tasks/?service=YOUR_SERVICE_NAME
      expr: |
        sum (twTasks_health_tasksInErrorCountPerType{service="YOUR_SERVICE_NAME"}) > 0
      severity: warning
      repeatInterval: 6h
      runbookURL: YOUR_RUN_BOOK_URL
      slackChannel: '#YOUR_SLACK_CHANNEL'
      dashboardURL: https://dashboards.tw.ee/d/6cf10e05-ed6e-4f91-9f68-5ca1f97a83d7/tw-tasks?var-service=YOUR_SERVICE_NAME
```

## Additional Configuration

TwTasks expects `tw-incidents` and `tw-graceful-shutdown` dependencies to be present and activated.

TwTasks works more efficiently with a proper JTA transaction manager than with Spring default one, even with only one database.

Engine configuration is described with and in `com.transferwise.tasks.TasksProperties`.

When using additional shards to the default one, TwTasks must be told about them by setting the
`additional-processing-buckets` property within the engine configuration. **Failure to do so will result in tasks
submitted to those buckets not being processed, and instead being sent to the error state.**

When using multiple shards, a shard can further configured by registering a `com.transferwise.tasks.buckets.BucketProperties` instance via
`com.transferwise.tasks.buckets.IBucketsManager.registerBucketProperties`.

When Kafka does not allow topics to be auto created, following topics have to be created by hand:
- `twTasks.<group-id>.executeTask.default`

And one topic for each additional bucket:
- `twTasks.<group id>.executeTask.<bucket id>`.

Reference application where a configuration can be basically copied from is `demoapp`.

To be able to see a task payload in production you have to configure which roles are able to do it. Any user with a suitable role will be
able to see tasks plain payload, but the user and what data they queried will be logged.
- `tw-tasks.core.tasks-management.view-task-data-roles`

```yaml
tw-tasks:
  core:
    tasks-management:
      view-task-data-roles:
        - ROLE_PAYIN_DEVEL
        - ROLE_TW_TASK_VIEW
```

You can also set type-level configuration for roles allowed. These will override the global configuration above.
- `tw-tasks.core.tasks-management.type-specific`

```yaml
tw-tasks:
  core:
    tasks-management:
      type-specific:
        -
          task-type: "myTaskType"
          view-task-data-roles:
            - ROLE_PAYIN_DEVEL
```

### Custom Kafka Configuration
`tw-tasks` library can be configured with custom Kafka config by defining `TwTasksKafkaConfiguration` bean in your application:
```java
@Bean
public TwTasksKafkaConfiguration twTaskKafkaConfiguration() {
    KafkaProperties props = ...;
    KafkaTemplate<String, String> template = ...;

    return new TwTasksKafkaConfiguration(props, template);
}
```
