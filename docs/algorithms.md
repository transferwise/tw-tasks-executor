# Algorithms Used
Some engineers want to understand how the engine actually works and which algorithms and tricks are used.

1. Post Transaction Hook.
   When a task is added, we register a post transaction hook. Immediately, after a commit has finalized, the task is triggered. In
   a case of rollback, nothing is done and can't be done, because the task record is "removed" from database.
   Currently there is one hook per task registered (for simplicity).

This approach is allowing us to skip the database polling on happy flow and basically have minimum latency for starting the
task processing.

2. Triggering Tasks with Kafka.
   First, it is important to understand, the task processing have its own consistency guarantees. When a node tries to move
   task from SUBMITTED->PROCESSING, it needs to also provide the expected version. If the task version has changed, this operation fails.
   Basically an atomic operation using optimistic locking with a version field. So any node can try to move a task to processing
   (we call it grabbing a task), at the same time, multiple times; but only one grabbing succeeds.

Now, because the task grabbing is safe and consistent, we can use more relaxed triggering mechanism, i.e. this does not have to
be consistent, can contain duplicate triggers and what not.

So the Kafka topics were chosen as a triggering channels. Every shard has its own triggering topic and the triggering message
is basically very small and concrete - "Trigger task with id X and version Y."

Any node, which receives this message will try to trigger the task.

3. Look-ahead triggerings fetching and processing.
   The number of topics, or actually the number of partitions is actually a scarce resource in Kafka. The limitation is not
   coming from Kafka itself, but from the Zookeeper underneath it.

One can write a system, where for example every task type and task priority has its own triggering topic, but this will not scale.

So an approach where we have one triggering topic (per shard), but we fetch a lot from the queue into memory, was choosed.
We have of course an upper limit how far ahead can we look.

This kind of look ahead fetch into memory allows us to "sort" tasks by priority, available processing power per type and so on.

4. Sorted task triggerings table.
   Triggerings are fetched into very specific data structure, which goal is to provide a next task for processing with maximum of logN complexity.

From this data structure, the system is finding a next task for execution. If it finds one, it stops, schedules the task execution in another thread and
starts again (searches a next task).

There is one table per priority. Next task is always searched from highest priority table to lowest. If the task type has
no available processing slots in higher priority, it is assumed, it can not have it in lower priority as well (very reasonable assumption).

Table for one priority looks like following.

SEND_EMAIL            | SEND_KAFKA_MESSAGE | EXECUTE_PAYOUT
----------------------|--------------------|---------------
1                     | 2                  | 4
3                     | 5                  |

The numbers are sequence numbers, i.e. the order in which task triggering was received from Kafka (most of the time, also the
order in which tasks were added).

Table is always keeping the order, that type with lowest order tasks are always left. For example, if we would take SEND-EMAIL-1 out,
the first and second column will change places.

The algorithm searches from left to right and looks only the first sequence number in specific type's queue. Then it tries
to book a processing slot for it's type. If processing slots are full, it tries with next type. If a type has been marked as full in
higher priority table, the type is not even checked anymore.

When we successfully booked a processing slot we remove that task from the table. The node will try to grab the task (via optimistic locking).
If grabbing succeeds, the task will be processed. If grabbing fails, we assume that another node already grabbed it or
manual intervention happened and we just ignore it.

So if for above table, the SEND_EMAIL processing slots where full, but we were able process SEND_KAFKA_MESSAGE, the table looks like

SEND_EMAIL            | EXECUTE_PAYOUT     | SEND_KAFKA_MESSAGE
----------------------|--------------------|---------------
1                     | 4                  | 5
3                     |                    |

5. Concurrency control with booking system
   A booking system was decided to use. It is very simple, yet very powerful.

Basically the engine will try to book a processing slot, before grabbing and executing a task. And once the task has been executed
it will free (de-book) the slot so it will be available for next tasks.

The booking registration is handled by implementation of `ITaskConcurrencyPolicy` where in its simples form it can be just
increasing and decreasing an atomic integer (`SimpleTaskConcurrencyPolicy`). On the other end of complexity spectrum, it could
be reserving and freeing Zookeeper's Semaphores to have cluster wide concurrency limit. Or have so called concurrency hierarchy, for
example 30 total, but 15 total for payout tasks, but only 3 total for one partner payout executions.

6. Programmatic configuration
   An engineer can provide different Task handling policies for controlling retries, concurrency, sharding and they all
   can be (re) implemented having as simple or complex logic as needed. Something which is extremely hard to achieve with
   property based config.
   It is always possible to write another layer providing full property based config on top of it, but the value added is
   very questionable.

7. Cluster nodes are not aware of each other
   The approach where cluster nodes communicate with each other to make decisions will never work in the long run (hazelcast, jgroups).
   In a good micro service architecture, the service cluster nodes should only communicate and indirectly coordinate their work with other separate shared
   services (kafka, zookeeper, database).

8. Database polling
   Database polling is avoided for happy flows, however no better algorithm has come to mind for scheduled tasks or for checking
   if any task is stuck or in ERROR state.

So one node for each of the following tasks is picked and it does database polling.
1. Triggering Scheduled (WAITING state) tasks.
   By default after every 5 seconds one node polls from database WAITING tasks which next_event_time has passed, and sends
   out Triggering Events into Kafka.
2. Cleaning old tasks.
   One node is polling database for old tasks and if found, deletes them.
3. Finding and resuming stuck tasks (Self healing)
   One node is polling database for tasks in not WAITING or final states where next_event_time has passed. It logs out
   a warning and resumes those.
4. Incidents generator
   Because the tw-incidents does not have leader election (Yet), all nodes are currently quering database for stuck tasks (which haven't self healed)
   and erroneous tasks and generating alerts (e.g. into VictorOps) for those.