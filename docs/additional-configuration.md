# Additional Configuration

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