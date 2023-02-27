# Usage

##### Add dependencies
```groovy
implementation("com.transferwise.tasks:tw-tasks-core-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-kafka-listener-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-management-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-core-test:${twTasksVersion}")
```

##### Create database tables
- [MySQL/MariaDB/Percona](tw-tasks-core/src/main/resources/db/changelog/db.tw-tasks-mysql.xml)
- [Postgres](tw-tasks-core/src/main/resources/db/changelog/db.tw-tasks-postgres.xml)

##### Add at least following configuration options
```yml
tw-tasks:
  core:
    group-id: MyFancyServiceName # Use only symbols suitable for Kafka topic!
    db-type: MYSQL
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
```yaml
tw-incidents:
  victorops:
    enabled: true
    notify-base-url: https://alert.victorops.com/integrations/generic/12345678/alert/
    routing-key: my-fancy-team
    api-token: my api token that comes after the "/alert/" bit in the Victorops URL to notify
```
