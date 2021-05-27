1. Investigate the feasibility of completely moving from ZonedDateTime to Instant.

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

8. Rename buckets and processing buckets to shards.
Much better understandable term. Not many people are aware that in hashmap (where the term bucket is coming from), the shards are called buckets.

16. unique_task_keys table should also support taskIds, it currently works only with string keys.

17. Rename property `tw-tasks.zookeeper-connect-string` to `tw-tasks.kafka.zookeeper.connect-string`, because it is only used for Kafka
topics configurations. Could refactor the properties to more hierarhical structure.

18. Move everything to taskVersionId ,instead of just separate id and version.

20. Add metric for how long it takes a task from adding to processing. Or scheduling time to processing.

23. Start using Avro or other binary messages for triggering queue. This Json crap is expensive?
