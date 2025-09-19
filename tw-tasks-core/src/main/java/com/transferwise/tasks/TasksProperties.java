package com.transferwise.tasks;

import com.transferwise.common.baseutils.validation.LegacyResolvedValue;
import com.transferwise.common.baseutils.validation.ResolvedValue;
import com.transferwise.tasks.utils.ClientIdUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
public class TasksProperties {

  /**
   * Unique id for service in the whole Company infrastructure.
   */
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String groupId;
  /**
   * Unique node id in the service cluster. It helps to make crash recovery for a node very fast, but also is good for logging and tracking reasons.
   */
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String clientId = ClientIdUtils.clientIdFromHostname();
  /**
   * How often do we check if any task is stuck.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration stuckTasksPollingInterval = Duration.ofMinutes(1);
  /**
   * How often do we try to clean very old tasks from the database.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration tasksCleaningInterval = Duration.ofSeconds(1);
  /**
   * How often do we check if any scheduled task should be executed now.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration waitingTasksPollingInterval = Duration.ofSeconds(5);
  /**
   * Generic maximum time to wait for any lock, event or polling. It helps to make the system more robust and better debuggable. Usually you will
   * never want to change this.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration genericMediumDelay = Duration.ofSeconds(5);
  /**
   * How often do we async commit Kafka triggers offsets.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration triggersCommitInterval = Duration.ofSeconds(5);

  /**
   * By default, how long should we expect a task to remain in any state, before we consider it as stuck.
   *
   * <p>Notice, that it is not used for PROCESSING state, where the maximum time is asked from task handler itself.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration taskStuckTimeout = Duration.ofMinutes(30);
  /**
   * How much do we load triggers from triggering topic into memory, aka look-ahead amount.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1L)
  private int maxTriggersInMemory = 10_000;
  /**
   * How many triggers maximum do we retrieve from Kafka with one polling loop.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1L)
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
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String zookeeperConnectString;
  /**
   * Topic replication factor for listened topics and task triggering topics.
   */
  private short topicReplicationFactor = 3;
  /**
   * MySQL or Postgres.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private DbType dbType;
  /**
   * MDC keys config.
   */
  @Valid
  @jakarta.validation.Valid
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Mdc mdc = new Mdc();
  /**
   * We support Transferwise Kafka failover, where for every topic, we additionally listen to 2 other topics, one starting with "fra." and other with
   * ".aws.".
   *
   * <p>e.g. kafkaDataCenterPrefixes = "fra.,aws.";
   */
  @ResolvedValue
  @LegacyResolvedValue
  private String kafkaDataCenterPrefixes = "";
  /**
   * Sometimes environments and engineers are forced to use same Kafka server, but still want to deal with only their own messages. In that case we
   * can configure a so called namespace string, which is prepended to every topics name.
   */
  @ResolvedValue
  @LegacyResolvedValue
  private String kafkaTopicsNamespace;

  /**
   * Set it to false, if you use proper transaction manager and not spring's default one. You will get better performance and waste less memory.
   * Default option true prevents possible deadlocks with any transaction manager.
   */
  private boolean asyncTaskTriggering = true;
  /**
   * Tied to the previous option. If asyncTaskTriggering is enabled, how many triggerings to we keep in memory, before starting to throttle new tasks
   * added.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1L)
  private int maxAsyncTaskTriggerings = 100000;
  /**
   * In how many threads to we try to trigger tasks when using crappy Spring own transaction manager.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1L)
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
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String autoResetOffsetTo = "-PT30M";
  /**
   * When do we consider a task or task unique key old enough to be removed from the database.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
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
  @Min(1L)
  @jakarta.validation.constraints.Min(1L)
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
  @NotNull
  @jakarta.validation.constraints.NotNull
  private Duration stuckTaskAge = Duration.ofMinutes(5);

  private boolean checkVersionBeforeGrabbing = false;

  private boolean assertStatusOnGrabbing = false;

  /**
   * The additional task buckets, not including the default bucket, that we will process.
   *
   * <p>If a task handler is configured with a bucket not present in this list, then the handler will not be invoked when new tasks of the configured
   * type are submitted, and instead the task will be sent to the error state.
   */
  @NotNull
  @jakarta.validation.constraints.NotNull
  private List<String> additionalProcessingBuckets = new ArrayList<>();

  /**
   * Need to make it configurable as in some environments, like smoke tests, we don't need a zookeeper connection.
   */
  private boolean preventStartWithoutZookeeper = true;

  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String taskTableName = "tw_task";
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String uniqueTaskKeyTableName = "unique_tw_task_key";
  @NotBlank
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String taskDataTableName = "tw_task_data";
  @NotNull
  @jakarta.validation.constraints.NotNull
  @ResolvedValue
  @LegacyResolvedValue
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
  @ResolvedValue
  @LegacyResolvedValue
  private String baseUrl;

  /**
   * Safety limit, to not kill database performance, when something goes horribly wrong. For example when we have millions of waiting, erronous or
   * stuck tasks.
   *
   * <p>The side effect is, that for example erroneous tasks count will never exceed this number.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1)
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
   * <p>It makes the cleaning process a bit less efficient, and it is almost never needed.
   */
  private boolean paranoidTasksCleaning = false;

  /**
   * If true, task cleaner will be started.
   *
   * <p>It starts to run task cleaner.
   */
  private boolean startTasksCleaner = true;

  /**
   * If true, task resumer will be started automatically.
   *
   * <p>It starts to run task resumer.
   */
  private boolean startTaskResumer = true;

  /**
   * How many tasks per bucket we are trying to grab at the same time.
   *
   * <p>Mainly meant as a soft safety measure in cases where concurrency policies are lacking good quality.
   *
   * <p>The amount of tasks grabbings happening at the same time, is also limited by the concurrency policies.
   *
   * <p>The higher the latency between application and the database, the more useful a larger number can be.
   *
   * <p>The default 25 is somewhat optimized for RDS Multi A/Z databases with high commit latency, where we have 6 nodes application cluster,
   * relatively close to the database.
   */
  @Min(1L)
  @jakarta.validation.constraints.Min(1)
  private Integer taskGrabbingMaxConcurrency = 25;

  /**
   * Cluster wide tasks state monitoring options.
   */
  @Valid
  @jakarta.validation.Valid
  private ClusterWideTasksStateMonitor clusterWideTasksStateMonitor = new ClusterWideTasksStateMonitor();

  /**
   * Task types for which some tasks are still present in the database, and still yet to be executed. This will allow a NO-OP task
   * handler to pick them up and execute them gracefully without creating noise for the service owners.
   */
  private List<String> noOpTaskTypes;

  public enum DbType {
    MYSQL, POSTGRES
  }

  @Valid
  @jakarta.validation.Valid
  private TasksManagement tasksManagement = new TasksManagement();

  @Valid
  @jakarta.validation.Valid
  private Compression compression = new Compression();

  @Valid
  @jakarta.validation.Valid
  private Environment environment = new Environment();

  @Valid
  @jakarta.validation.Valid
  private Triggering triggering = new Triggering();

  @Valid
  @jakarta.validation.Valid
  private TasksResumer tasksResumer = new TasksResumer();

  @Data
  public static class Triggering {

    @Valid
    @jakarta.validation.Valid
    private Kafka kafka = new Kafka();

    @Data
    public static class Kafka {

      @NotBlank
      @jakarta.validation.constraints.NotBlank
      @ResolvedValue
      @LegacyResolvedValue
      private String bootstrapServers;

      /**
       * Allows to override configuration properties for both Kafka Consumers. and Producers.
       */
      private Map<String, String> properties = new HashMap<>();
    }

  }

  @Data
  public static class TasksManagement {

    /**
     * A role for viewing PII data.
     */
    @NotNull
    @jakarta.validation.constraints.NotNull
    private Set<String> viewTaskDataRoles = new HashSet<>(Collections.singletonList("NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY"));
    /**
     * Roles for all other task management endpoints.
     */
    @NotNull
    @jakarta.validation.constraints.NotNull
    private Set<String> roles = new HashSet<>(Collections.singleton("ROLE_DEVEL"));

    @NotNull
    @jakarta.validation.constraints.NotNull
    @Valid
    @jakarta.validation.Valid
    private List<TypeSpecificTaskManagement> typeSpecific = Collections.emptyList();

    /**
     * Services with lots of tasks might cause the endpoint to timeout, service owners may disable it to avoid high db load.
     **/
    private boolean enableGetTaskTypes = true;

    @Data
    public static class TypeSpecificTaskManagement {

      @NotBlank
      @jakarta.validation.constraints.NotBlank
      @ResolvedValue
      @LegacyResolvedValue
      private String taskType;
      @NotEmpty
      @jakarta.validation.constraints.NotEmpty
      private Set<String> viewTaskDataRoles = new HashSet<>(Collections.singletonList("NONEXISTING_ROLE_FOR_TESTING_PURPOSES_ONLY"));
    }
  }

  /**
   * Allows to specify MDC keys used.
   */
  @Data
  public static class Mdc {

    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String taskIdKey = "twTaskId";
    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String taskVersionKey = "twTaskVersion";
    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String taskTypeKey = "twTaskType";
    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
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
    @NotNull
    @jakarta.validation.constraints.NotNull
    private Duration interval = Duration.ofSeconds(30);
    /**
     * The time between monitor acquires leadership and first check is done.
     */
    @NotNull
    @jakarta.validation.constraints.NotNull
    private Duration startDelay = Duration.ofSeconds(5);
    /**
     * If enabled, we will gather approximate tasks and unique keys counts from database information schema tables.
     */
    private boolean tasksCountingEnabled = true;
  }

  @Data
  @Accessors(chain = true)
  public static class Compression {

    @NotNull
    @jakarta.validation.constraints.NotNull
    private CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;

    /**
     * Can be quite large, even when we have small(er) messages, because we reuse memory buffers.
     */
    private Integer blockSizeBytes;

    /**
     * Approximate message size is considered.
     */
    private int minSize = 128;

    /**
     * Used when applicable.
     */
    private Integer level;
  }

  @Data
  @Accessors(chain = true)
  public static class Environment {

    /**
     * Version deployed (e.g. to production).
     *
     * <p>Allows tw-tasks to decide when it should fail fast, instead of risking with incompatibilities or/and processing pauses.
     */
    @NotBlank
    @jakarta.validation.constraints.NotBlank
    @ResolvedValue
    @LegacyResolvedValue
    private String previousVersion;

  }

  @Data
  @Accessors(chain = true)
  public static class TasksResumer {

    /**
     * Specifies how many tasks we are loading from the database in one go to be then resumed concurrently.
     */
    @Positive
    @jakarta.validation.constraints.Positive
    private int batchSize = 1000;

    @Positive
    @jakarta.validation.constraints.Positive
    private int concurrency = 10;
  }
}
