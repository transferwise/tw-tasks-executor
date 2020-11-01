package com.transferwise.tasks;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Data;
import org.springframework.validation.Errors;
import org.springframework.validation.ValidationUtils;

@Data
public class TasksProperties {

  /**
   * Unique id for service in the whole Company infrastructure.
   */
  private String groupId;
  /**
   * Unique node id in the service cluster. It helps to make crash recovery for a node very fast, but also is good for logging and tracking reasons.
   */
  private String clientId;
  /**
   * How often do we check if any task is stuck.
   */
  private Duration stuckTasksPollingInterval = Duration.ofMinutes(1);
  /**
   * How often do we try to clean very old tasks from the database.
   */
  private Duration tasksCleaningInterval = Duration.ofSeconds(15);
  /**
   * How often do we check if any scheduled task should be executed now.
   */
  private Duration waitingTasksPollingInterval = Duration.ofSeconds(5);
  /**
   * Generic maximum time to wait for any lock, event or polling. It helps to make the system more robust and better debuggable. Usually you will
   * never want to change this.
   */
  private Duration genericMediumDelay = Duration.ofSeconds(5);
  /**
   * By default, how long should we expect a task to remain in any state, before we consider it as stuck.
   *
   * <p>Notice, that it is not used for PROCESSING state, where the maximum time is asked from task handler itself.
   */
  private Duration taskStuckTimeout = Duration.ofMinutes(30);
  /**
   * How much do we load triggers from triggering topic into memory, aka look-ahead amount.
   */
  private int maxTriggersInMemory = 100000;
  /**
   * How many triggers maximum do we retrieve from Kafka with one polling loop.
   */
  private int triggerFetchSize = 100;
  /**
   * How many nodes do we expect to be in the cluster.
   */
  private int maxNodeCount = 2;
  /**
   * We have two triggering algorithms. First one spreads task triggerings into partitions and every service node is listening only its own partition.
   * The cons is that if one node gets very slow, some tasks latency goes up, even when other nodes would have processing power for them. In second
   * algorithms every node is taking every trigger from topic and tries to execute them. The cons is that it may be inefficient with large cluster
   * size (10+), even when we actually use very efficient optimistic locking for a node to grab a task for itself.
   *
   * <p>It does not work well in cluster, where node ids change. Like, for example, in kubernetes. So if service in kubernetes don't set it to true.
   *
   * <p>So rule of thumb is that you probably want to use the second algorithm for cluster with less than 10 nodes.
   *
   * <p>Second algorithm is activated with setting this parameter to true.
   */
  private boolean triggerSameTaskInAllNodes = false;
  /**
   * Connection string to Zookeeper. Used to set partition sizes for different topics.
   */
  private String zookeeperConnectString;
  /**
   * Topic replication factor for listened topics and task triggering topics.
   */
  private short topicReplicationFactor = 3;
  /**
   * MySQL or Postgres.
   */
  private DbType dbType;
  /**
   * MDC keys config.
   */
  private Mdc mdc = new Mdc();
  /**
   * We support Transferwise Kafka failover, where for every topic, we additionally listen to 2 other topics, one starting with "fra." and other with
   * ".aws.".
   *
   * <p>e.g. kafkaDataCenterPrefixes = "fra.,aws.";
   */
  private String kafkaDataCenterPrefixes = "";
  /**
   * Sometimes environments and engineers are forced to use same Kafka server, but still want to deal with only their own messages. In that case we
   * can configure a so called namespace string, which is prepended to every topics name.
   */
  private String kafkaTopicsNamespace;
  /**
   * When listening Kafka Topics, it is possible to specify the topics replication factor and partitions count, which is applied on application
   * startup.
   *
   * <p>It may add a small additional time for startup, which is sometimes preferred to be avoided (integration tests, development).
   *
   * <p>Using this option, the automatic topic configuration can be turned off.
   *
   * @deprecated used exclusively by ext-kafka-listener which is deprecated and soon will be removed
   */
  @Deprecated
  private boolean coreKafkaListenerTopicsConfiguringEnabled = true;

  /**
   * Set it to false, if you use proper transaction manager and not spring's default one. You will get better performance and waste less memory.
   * Default option true prevents possible deadlocks with any transaction manager.
   */
  private boolean asyncTaskTriggering = true;
  /**
   * Tied to the previous option. If asyncTaskTriggering is enabled, how many triggerings to we keep in memory, before starting to throttle new tasks
   * added.
   */
  private int maxAsyncTaskTriggerings = 100000;
  /**
   * In how many threads to we try to trigger tasks when using crappy Spring own transaction manager.
   */
  private int asyncTaskTriggeringsConcurrency = 10;
  /**
   * Highest task priority allowed.
   */
  private int highestPriority = 0;
  /**
   * Lowest task priority allowed.
   */
  private int lowestPriority = 9;
  /**
   * When we lose the offset of a triggering topic, where do we rewind? Only used for task triggering. For usual topics listeners, the spring-kafka
   * configuration is used.
   *
   * <p>Can use "earliest", "latest" or Duration notion. For example, if you want to rewind 30 min back, you should write "-PT30M";
   */
  private String autoResetOffsetTo = "-PT30M";
  /**
   * When do we consider a task or task unique key old enough to be removed from the database.
   */
  private Duration finishedTasksHistoryToKeep = Duration.ofDays(30);
  /**
   * How many old tasks maximum do we delete in one batch/transaction. Deletion should always happen in small batches to not create too big spikes for
   * database replication.
   *
   * <p>This can handle 10 tasks/s.
   *
   * <p>TODO: Implement dynamic, adaptive configuration/system for that instead. Batch Size could be constant, but interval should learn from current
   * situation. Can use TCP/IP flow control algorithms.
   */
  private int tasksHistoryDeletingBatchSize = 2 * 125;

  //TODO: This does not make sense as generic parameter.
  //      taskhandler should provide this info programmatically.
  //      No usage so far in Tw
  /**
   * Should we delete a task immediately after it has marked as DONE. Maybe in the future the task handler can provide this information.
   *
   * <p>The value of this property depends on balance between keeping the storage used minimal or being able to later track,
   * analyze or even force-retry executed tasks.
   *
   * <p>Notice, that if you set it to true, you currently lose the taskId based uniqueness checks.
   */
  private boolean deleteTaskOnFinish = false;

  /**
   * Removes the payload but keeps task record in database. Useful for huge payloads where uniqueness checks are still desired.
   */
  private boolean clearPayloadOnFinish = false;

  /**
   * How long a task has to be stuck, before we start sending out VictorOps alerts.
   */
  private Duration stuckTaskAge = taskStuckTimeout;

  private boolean checkVersionBeforeGrabbing = false;

  /**
   * The additional task buckets, not including the default bucket, that we will process.
   *
   * <p>If a task handler is configured with a bucket not present in this list, then the handler will not be invoked when new tasks of the configured
   * type are submitted, and instead the task will be sent to the error state.
   */
  private List<String> additionalProcessingBuckets = new ArrayList<>();

  private boolean configureKafkaTopics = false;

  /**
   * Need to make it configurable as in some environments, like smoke tests, we don't need a zookeeper connection.
   */
  private boolean preventStartWithoutZookeeper = true;

  private String taskTableName = "tw_task";
  private String uniqueTaskKeyTableName = "unique_tw_task_key";
  private String taskTablesSchemaName = "";

  /**
   * For extremely latency sensitive scenarios or for tests, we allow to trigger directly in the same process, instead of going through the kafka
   * pipes. TODO: Maybe allow to execute service tests without having no Kafka at all. Probably best to have a separate ITasksExecutionTrigger
   * implementation instead of hacking it into Kafka one.
   */
  private boolean triggerInSameProcess;

  /**
   * Just to allow `ignoreUnknownFields` work.
   */
  private String baseUrl;

  /**
   * Safety limit, to not kill database performance, when something goes horribly wrong. For example when we have millions of waiting, erronous or
   * stuck tasks.
   *
   * <p>The side effect is, that for example erroneous tasks count will never exceed this number.
   */
  private int maxDatabaseFetchSize = 10000;

  /**
   * Allows to turn off automatic start of tasks processing. In technical terms, allows to turn off fetching of task triggers and processing those.
   *
   * <p>Does not apply when tasks are triggered with `triggerInSameProcess` system.
   */
  private boolean autoStartProcessing = true;

  /**
   * Experimental, do not use.
   */
  private Duration interruptTasksAfterShutdownTime = null;

  /**
   * Adds more counters showing more details for the processing engine.
   *
   * <p>Adds a considerable overhead and interpreting results needs deep understanding of tw-tasks code.
   *
   * <p>Meant to be used only by tw-tasks contributors when helping to solve some very specific incident.
   */
  private boolean debugMetricsEnabled = false;

  /**
   * Code is running some assertions.
   *
   * <p>Only meant to be true in tw-tasks own test-suite.
   */
  private boolean assertionsEnabled = false;

  /**
   * If true, the task cleaning will also handle those cases consistently where just-to-be deleted tasks may change.
   *
   * <p>It makes the cleaning process a bit less efficient and it is almost never needed.
   */
  private boolean paranoidTasksCleaning = false;

  /**
   * Cluster wide tasks state monitoring options.
   */
  private ClusterWideTasksStateMonitor clusterWideTasksStateMonitor = new ClusterWideTasksStateMonitor();

  public enum DbType {
    MYSQL, POSTGRES
  }

  private TasksManagement tasksManagement = new TasksManagement();

  public static class Validator implements org.springframework.validation.Validator {

    @Override
    public boolean supports(Class<?> clazz) {
      return TasksProperties.class == clazz;
    }

    @Override
    public void validate(Object target, Errors errors) {
      ValidationUtils.rejectIfEmpty(errors, "groupId", "groupId.empty");
      ValidationUtils.rejectIfEmpty(errors, "clientId", "clientId.empty");
      ValidationUtils.rejectIfEmpty(errors, "zookeeperConnectString", "zookeeperConnectString.empty");
      ValidationUtils.rejectIfEmpty(errors, "dbType", "dbType.empty");
    }
  }

  @Data
  public static class TasksManagement {

    /**
     * A role for viewing PII data.
     */
    private Set<String> viewTaskDataRoles = new HashSet<>(Collections.singletonList("NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY"));
    /**
     * Roles for all other task management endpoints.
     */
    private Set<String> roles = new HashSet<>(Collections.singleton("ROLE_DEVEL"));
  }

  /**
   * Allows to specify MDC keys used.
   */
  @Data
  public static class Mdc {

    private String taskIdKey = "twTaskId";
    private String taskVersionKey = "twTaskVersion";
    private String taskTypeKey = "twTaskType";
    private String taskSubTypeKey = "twTaskSubType";
  }

  /**
   * Cluster-wide monitoring config.
   */
  @Data
  public static class ClusterWideTasksStateMonitor {

    /**
     * How often does the monitor approximately run.
     *
     * <p>Monitor can actually run slower or faster, when leadership is switching rapidly.
     */
    private Duration interval = Duration.ofSeconds(30);
    /**
     * The time between monitor acquires leadership and first check is done.
     */
    private Duration startDelay = Duration.ofSeconds(5);
    /**
     * If enabled, we will gather approximate tasks and unique keys counts from database information schema tables.
     */
    private boolean tasksCountingEnabled = true;
  }
}
