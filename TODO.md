1. Investigate the feasibility of completely moving from ZonedDateTime to Instant.

2. Go open source.

3. Some Strings are repeated. For example Task's type, bucket's id, Kafka topics.
In order to conserve memory or allow bigger buffers, we could String.intern() them as soon as they are loaded into memory.
Needs performance testing.

4. Task triggering latency depends on Kafka. It is usually very low, but if someone needs absolute minimum,
we could consider triggering immediately on the same node which created the task.

It can be especially useful for test environments.

5. Rethink of using Zookeeper's semaphores for concurrency control, for example when cluster wide max concurrency
 is needed. Currently it does not seem to work on all situations (crashes, network partitions) and workaround is
 to specify the concurrency N/nodeCount. Also test how fast it is, because current task triggering table algorithm assumes,
 that booking a processing slot is very fast (mostly based on atomic numbers).

7. Refactor everything into smaller layers/components. This is a precondition for going open source.
Layers and components could look like following (lowest to highest)

- Commons and Utils
- Test Helpers
- Zookeeper Helpers, Kafka Helpers, MetricsHelper
- Tw Tasks Engine, Tw Tasks Test Helpers (TestTasksService)
- ToKafka messages, FromKafka messages, Periodic Tasks (Cron)

8. Rename buckets and processing buckets to shards.
Much better understandable term. Not many people are aware that in hashmap (where the term bucket is coming from), the shards are called buckets.

10. Try to get rid of KafkaTemplate and use a TwTasks defined Provider interface instead.

11. Consider the need for Tasks' error handlers.
Currently there is no strong need for it, because this can be done with proper retry policy and task processor combination.
However error handlers would be cleaner solution. But those error handlers would need their own retries policies and what not,
and in the end it may not make things cleaner.

13. Move task payload to separate table for better efficiency with default MySQL binlog settings.

14. Add possibility for binary payload.

16. unique_task_keys table should also support taskIds, it currently works only with string keys.

17. Rename property `tw-tasks.zookeeper-connect-string` to `tw-tasks.kafka.zookeeper.connect-string`, because it is only used for Kafka
topics configurations. Could refactor the properties to more hierarhical structure.

18. Move everything to taskVersionId ,instead of just separate id and version. Also always log both out and set them both in MDC.


20. Add metric for how long it takes a task from adding to processing. Or scheduling time to processing.

23. Start using Avro or other binary messages for triggering queue. This Json crap is expensive?
