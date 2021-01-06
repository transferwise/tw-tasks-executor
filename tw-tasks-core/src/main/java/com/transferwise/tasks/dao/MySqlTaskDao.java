package com.transferwise.tasks.dao;

import static com.transferwise.tasks.utils.TimeUtils.toZonedDateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer.SerializedData;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.helpers.sql.ArgumentPreparedStatementSetter;
import com.transferwise.tasks.helpers.sql.CacheKey;
import com.transferwise.tasks.helpers.sql.SqlHelper;
import com.transferwise.tasks.utils.TimeUtils;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

//TODO: We should delete state_time all together. time_updated should be enough.
//TODO: Move everything to TaskVersionId, instead of separate version and taskId. Additional mem alloc shouldn't be a
// problem.
//TODO: Replace all methods setting status and nextEventTime with one single method.
//TODO: Replace StuckTask with BaseTask1.
//TODO: Move methods only used in tests to separate DAO class?
//TODO: Add check, that updatedCount is not more than intended?
//TODO: Separate interface methods to Engine, Management and Tests and annotate them.
//TODO: We should allow to set "data" field to null, to have better performance on Postgres.
@Slf4j
public class MySqlTaskDao implements ITaskDao {

  private static final String DELETE_TASKS_BY_ID_BATCHES = "deleteTasksByIdBatchesSql";
  private static final String DELETE_UNIQUE_TASK_KEYS_BY_ID_BATCHES = "deleteUniqueTaskKeysByIdBatchesSql";
  private static final String DELETE_TASK_DATAS_BY_ID_BATCHES = "deleteTaskDatasByIdBatchesSql";

  @Autowired
  protected TasksProperties tasksProperties;
  @Autowired
  protected ITaskDaoDataSerializer taskDataSerializer;

  private final ConcurrentHashMap<CacheKey, String> sqlCache = new ConcurrentHashMap<>();

  private final JdbcTemplate jdbcTemplate;
  private final ITaskSqlMapper sqlMapper;
  private final DataSource dataSource;
  private final IMeterHelper meterHelper;

  public MySqlTaskDao(DataSource dataSource, IMeterHelper meterHelper) {
    this(dataSource, new MySqlTaskTypesMapper(), meterHelper);
  }

  public MySqlTaskDao(DataSource dataSource, ITaskSqlMapper sqlMapper, IMeterHelper meterHelper) {
    this.dataSource = dataSource;
    jdbcTemplate = new JdbcTemplate(dataSource);
    this.sqlMapper = sqlMapper;
    this.meterHelper = meterHelper;
  }

  protected String insertTaskSql;
  protected String insertUniqueTaskKeySql;
  protected String insertTaskDataSql;
  protected String setToBeRetriedSql;
  protected String setToBeRetriedSql1;
  protected String grabForProcessingSql;
  protected String setStatusSql;
  protected String getStuckTasksSql;
  protected String prepareStuckOnProcessingTaskForResumingSql;
  protected String prepareStuckOnProcessingTaskForResumingSql1;
  protected String getTasksCountInStatusSql;
  protected String getTasksCountInErrorGroupedSql;
  protected String getStuckTasksCountSql;
  protected String getStuckTasksCountGroupedSql;
  protected String getTaskSql;
  protected String getTaskSql1;
  protected String getTaskSql2;
  protected String deleteTaskSql;
  protected String deleteUniqueTaskKeySql;
  protected String deleteTaskDataSql;
  protected String deleteFinishedOldTasksSql;
  protected String deleteFinishedOldTasksSql1;
  protected String clearPayloadAndMarkDoneSql;
  protected String getEarliesTaskNextEventTimeSql;
  protected String getTaskVersionSql;
  protected String deleteTasksByIdBatchesSql;
  protected String deleteUniqueTaskKeysByIdBatchesSql;
  protected String deleteTaskDatasByIdBatchesSql;
  protected String deleteFinishedOldTasksSql2;
  protected String getApproximateTasksCountSql;
  protected String getApproximateTasksCountSql1;
  protected String getApproximateUniqueKeysCountSql;
  protected String getApproximateUniqueKeysCountSql1;
  protected String getApproximateTaskDatasCountSql;
  protected String getApproximateTaskDatasCountSql1;

  protected final int[] questionBuckets = {1, 5, 25, 125, 625};

  protected final TaskStatus[] stuckStatuses = new TaskStatus[]{TaskStatus.NEW, TaskStatus.SUBMITTED, TaskStatus.WAITING, TaskStatus.PROCESSING};

  protected ITwTaskTables twTaskTables(TasksProperties tasksProperties) {
    return new MySqlTaskTables(tasksProperties);
  }

  @PostConstruct
  public void init() {
    ITwTaskTables tables = twTaskTables(tasksProperties);
    String taskTable = tables.getTaskTableIdentifier();
    String uniqueTaskKeyTable = tables.getUniqueTaskKeyTableIdentifier();
    String taskDataTable = tables.getTaskDataTableIdentifier();

    insertTaskSql = "insert ignore into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values (?,?,?,?,?,?,?,?,?,?,?,?)";
    insertUniqueTaskKeySql = "insert ignore into " + uniqueTaskKeyTable + "(task_id,key_hash,`key`) values (?, ?, ?)";
    insertTaskDataSql = "insert into " + taskDataTable + "(task_id,data_format,data) values (?,?,?)";
    setToBeRetriedSql = "update " + taskTable + " set status=?,next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    setToBeRetriedSql1 = "update " + taskTable + " set status=?,next_event_time=?"
        + ",processing_tries_count=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    grabForProcessingSql = "update " + taskTable + " set processing_client_id=?,status=?"
        + ",processing_start_time=?,next_event_time=?,processing_tries_count=processing_tries_count+1"
        + ",state_time=?,time_updated=?,version=? where id=? and version=? and status=?";
    setStatusSql = "update " + taskTable + " set status=?,next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    getStuckTasksSql = "select id,version,type,priority,status from " + taskTable + " where status=?"
        + " and next_event_time<? order by next_event_time limit ?";
    prepareStuckOnProcessingTaskForResumingSql =
        "select id,version,type,priority from " + taskTable + " where status=? and next_event_time>? and processing_client_id=?";
    prepareStuckOnProcessingTaskForResumingSql1 = "update " + taskTable + " set status=?,next_event_time=?"
        + ",state_time=?,time_updated=?,version=? where id=? and version=?";
    getTasksCountInStatusSql = "select count(*) from (select 1 from " + taskTable + " where status = ? order by next_event_time limit ?) q";
    getTasksCountInErrorGroupedSql = "select type, count(*) from (select type from " + taskTable + " where status='"
        + TaskStatus.ERROR.name() + "' order by next_event_time limit ?) q group by type";
    getStuckTasksCountSql = "select count(*) from (select 1 from " + taskTable + " where status=?"
        + " and next_event_time<? order by next_event_time limit ?) q";
    getStuckTasksCountGroupedSql = "select type, count(*) from (select type from " + taskTable + " where status=?"
        + " and next_event_time<? order by next_event_time limit ?) q group by type";
    getTaskSql = "select id,version,type,status,priority from " + taskTable + " where id=?";
    getTaskSql1 = "select id,version,type,status,priority,sub_type,t.data,processing_tries_count,d.data_format,d.data"
        + " from " + taskTable + " t left join " + taskDataTable + " d on t.id=d.task_id"
        + " where t.id=?";
    getTaskSql2 = "select id,version,type,status,priority,sub_type,t.data"
        + ",processing_tries_count,state_time,next_event_time,processing_client_id,d.data_format,d.data"
        + " from " + taskTable + " t left join " + taskDataTable + " d on t.id=d.task_id"
        + " where t.id=?";
    deleteTaskSql = "delete from " + taskTable + " where id=? and version=?";
    deleteUniqueTaskKeySql = "delete from " + uniqueTaskKeyTable + " where task_id=?";
    deleteTaskDataSql = "delete from " + taskDataTable + " where task_id=?";
    deleteTasksByIdBatchesSql = "delete from " + taskTable + " where id in (??)";
    deleteUniqueTaskKeysByIdBatchesSql = "delete from " + uniqueTaskKeyTable + " where task_id in (??)";
    deleteTaskDatasByIdBatchesSql = "delete from " + taskDataTable + " where task_id in (??)";
    deleteFinishedOldTasksSql = "select id,version from " + taskTable + " where status=? and next_event_time<? order by next_event_time limit ?";
    deleteFinishedOldTasksSql1 = "select next_event_time from " + taskTable + " where id=?";
    deleteFinishedOldTasksSql2 = "select id from " + taskTable + " where status=? and next_event_time<? order by next_event_time limit ?";
    clearPayloadAndMarkDoneSql = "update " + taskTable + " set data='',status=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    getEarliesTaskNextEventTimeSql = "select min(next_event_time) from " + taskTable + " where status=?";
    getTaskVersionSql = "select version from " + taskTable + " where id=?";
    getApproximateTasksCountSql = getApproximateTableCountSql(false, tasksProperties.getTaskTableName());
    getApproximateTasksCountSql1 = getApproximateTableCountSql(true, tasksProperties.getTaskTableName());
    getApproximateUniqueKeysCountSql = getApproximateTableCountSql(false, tasksProperties.getUniqueTaskKeyTableName());
    getApproximateUniqueKeysCountSql1 = getApproximateTableCountSql(true, tasksProperties.getUniqueTaskKeyTableName());
    getApproximateTaskDatasCountSql = getApproximateTableCountSql(false, tasksProperties.getTaskDataTableName());
    getApproximateTaskDatasCountSql1 = getApproximateTableCountSql(true, tasksProperties.getTaskDataTableName());
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public InsertTaskResponse insertTask(InsertTaskRequest request) {
    return ExceptionUtils.doUnchecked(() -> {
      Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
      ZonedDateTime nextEventTime = request.getRunAfterTime() == null ? request.getMaxStuckTime() : request.getRunAfterTime();

      boolean uuidProvided = request.getTaskId() != null;
      String key = request.getKey();
      boolean keyProvided = key != null;

      UUID taskId = uuidProvided ? request.getTaskId() : UuidUtils.generatePrefixCombUuid();

      if (keyProvided) {
        Integer keyHash = key.hashCode();
        int insertedCount = jdbcTemplate.update(insertUniqueTaskKeySql, args(taskId, keyHash, key));
        if (insertedCount == 0) {
          log.debug("Task with key '{}' and hash '{}' was not unique.", key, keyHash);
          return new InsertTaskResponse().setInserted(false);
        }
      }

      Connection con = DataSourceUtils.getConnection(dataSource);
      try {
        try (PreparedStatement ps = con.prepareStatement(insertTaskSql)) {
          String data = "";
          if (tasksProperties.isCopyDataToTwTaskField() && request.getData() != null) {
            data = new String(request.getData(), StandardCharsets.UTF_8);
          }
          args(taskId, request.getType(), request.getSubType(),
              request.getStatus(), data, nextEventTime, now, now, now, 0, 0, request.getPriority())
              .setValues(ps);

          int insertedCount = ps.executeUpdate();

          SQLWarning warnings = ps.getWarnings();

          if (insertedCount == 0) {
            if (!didInsertFailDueToDuplicateKeyConflict(warnings)) {
              throw new IllegalStateException("Task insertion did not succeed. The warning code is unknown: " + warnings.getErrorCode());
            }
            return new InsertTaskResponse().setInserted(false);
          } else if (warnings != null) {
            // Notice, that INSERT IGNORE actually does ignore everything, even null checks or invalid data.
            // For protecting sensitive data, we can only give out the error/vendor code.
            throw new IllegalStateException("Task insertion succeeded, but with warnings. Error code: " + warnings.getErrorCode());
          }
        }
      } finally {
        DataSourceUtils.releaseConnection(con, dataSource);
      }

      byte[] data = request.getData();
      if (data != null) {
        SerializedData serializedData = taskDataSerializer.serialize(data, request.getCompression());
        jdbcTemplate.update(insertTaskDataSql, args(taskId, Integer.valueOf(serializedData.getDataFormat()), serializedData.getData()));
        meterHelper.incrementCounter(IMeterHelper.METRIC_PREFIX + "dao.data.size", ImmutableMap.of("taskType", request.getType()), data.length);
        meterHelper.incrementCounter(IMeterHelper.METRIC_PREFIX + "dao.data.serialized.size", ImmutableMap.of("taskType", request.getType()),
            serializedData.getData().length);
      }

      return new InsertTaskResponse().setTaskId(taskId).setInserted(true);
    });
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean setToBeRetried(UUID taskId, ZonedDateTime retryTime, long version, boolean resetTriesCount) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));

    int updatedCount;
    if (resetTriesCount) {
      updatedCount = jdbcTemplate.update(setToBeRetriedSql1, args(TaskStatus.WAITING,
          retryTime, 0, now, now, version + 1, taskId, version));
    } else {
      updatedCount = jdbcTemplate.update(setToBeRetriedSql, args(TaskStatus.WAITING,
          retryTime, now, now, version + 1, taskId, version));
    }
    return updatedCount == 1;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public Task grabForProcessing(BaseTask task, String clientId, Instant maxProcessingEndTime) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));

    int updatedCount = jdbcTemplate.update(grabForProcessingSql, args(clientId, TaskStatus.PROCESSING, now,
        maxProcessingEndTime, now, now, task.getVersion() + 1, task.getId(), task.getVersion(), TaskStatus.SUBMITTED));
    if (updatedCount == 0) {
      return null;
    }
    return getTask(task.getId(), Task.class);
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean setStatus(UUID taskId, TaskStatus status, long version) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    int updatedCount = jdbcTemplate.update(setStatusSql, args(status, now, now, now, version + 1, taskId, version));
    return updatedCount == 1;
  }

  @Override
  public GetStuckTasksResponse getStuckTasks(int batchSize, TaskStatus status) {
    Timestamp now = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant());

    List<StuckTask> stuckTasks = jdbcTemplate.query(getStuckTasksSql, args(status, now, batchSize + 1), (rs, rowNum) ->
        new StuckTask()
            .setVersionId(new TaskVersionId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)), rs.getLong(2)))
            .setType(rs.getString(3))
            .setPriority(rs.getInt(4)).setStatus(rs.getString(5)));
    boolean hasMore = stuckTasks.size() > batchSize;
    if (hasMore) {
      stuckTasks.remove(stuckTasks.size() - 1);
    }
    return new GetStuckTasksResponse().setStuckTasks(stuckTasks).setHasMore(hasMore);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public ZonedDateTime getEarliestTaskNextEventTime(TaskStatus status) {
    return getFirst(jdbcTemplate.query(getEarliesTaskNextEventTimeSql, args(status), (rs, idx) ->
        TimeUtils.toZonedDateTime(rs.getTimestamp(1))));
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public List<StuckTask> prepareStuckOnProcessingTasksForResuming(String clientId, ZonedDateTime maxStuckTime) {
    Timestamp now = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant());
    List<StuckTask> result = new ArrayList<>();

    // We use this to make PostgreSql to always prefer the `(status, next_event_time)` index.
    // Otherwise we had cases were we ended up with db full-scan.
    Timestamp nextEventTimeFrom =
        Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).minus(tasksProperties.getStuckTaskAge().multipliedBy(2)).toInstant());

    jdbcTemplate.query(prepareStuckOnProcessingTaskForResumingSql, args(TaskStatus.PROCESSING, nextEventTimeFrom, clientId),
        rs -> {
          Object id = rs.getObject(1);
          long version = rs.getLong(2);
          int updatedCount = jdbcTemplate.update(prepareStuckOnProcessingTaskForResumingSql1, args(TaskStatus.SUBMITTED,
              maxStuckTime.toInstant(), now, now, version + 1, id, version));
          if (updatedCount == 1) {
            result.add(new StuckTask().setVersionId(new TaskVersionId(sqlMapper.sqlTaskIdToUuid(id), version + 1))
                .setType(rs.getString(3)).setStatus(TaskStatus.SUBMITTED.name())
                .setPriority(rs.getInt(4)));
          }
        });

    return result;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean markAsSubmitted(UUID taskId, long version, ZonedDateTime maxStuckTime) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    int updatedCount = jdbcTemplate.update(setStatusSql, args(TaskStatus.SUBMITTED, maxStuckTime, now, now, version + 1, taskId, version));
    return updatedCount == 1;
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public int getTasksCountInStatus(int maxCount, TaskStatus status) {
    List<Integer> results = jdbcTemplate.query(getTasksCountInStatusSql, args(status, maxCount),
        (rs, rowNum) -> rs.getInt(1));

    return DataAccessUtils.intResult(results);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public Map<String, Integer> getErronousTasksCountByType(int maxCount) {
    Map<String, Integer> resultMap = new HashMap<>();
    jdbcTemplate.query(getTasksCountInErrorGroupedSql, args(maxCount), rs -> {
      resultMap.put(rs.getString(1), rs.getInt(2));
    });
    return resultMap;
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public int getStuckTasksCount(ZonedDateTime age, int maxCount) {
    int cnt = 0;

    for (TaskStatus taskStatus : stuckStatuses) {
      List<Integer> results = jdbcTemplate.query(getStuckTasksCountSql, args(taskStatus, age, maxCount), (rs, rowNum) -> rs.getInt(1));

      cnt += DataAccessUtils.intResult(results);
    }

    return cnt;
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public Map<String, Integer> getStuckTasksCountByType(ZonedDateTime age, int maxCount) {
    Map<String, MutableInt> resultMap = new HashMap<>();
    for (TaskStatus taskStatus : stuckStatuses) {
      jdbcTemplate.query(getStuckTasksCountGroupedSql, args(taskStatus, age, maxCount), rs -> {
        resultMap.computeIfAbsent(rs.getString(1), k -> new MutableInt(0)).add(rs.getInt(2));
      });
    }
    return resultMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getValue()));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getTask(UUID taskId, Class<T> clazz) {
    if (taskId == null) {
      return null;
    }
    if (clazz.equals(BaseTask1.class)) {
      List<BaseTask1> result = jdbcTemplate.query(getTaskSql, args(taskId), (rs, rowNum) ->
          new BaseTask1().setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
              .setVersion(rs.getLong(2)).setType(rs.getString(3))
              .setStatus(rs.getString(4)).setPriority(rs.getInt(5)));
      return (T) getFirst(result);
    } else if (clazz.equals(Task.class)) {
      List<Task> result = jdbcTemplate.query(getTaskSql1, args(taskId), (rs, rowNum) -> {
        byte[] data = getData(rs, 7, 9, 10);
        return new Task().setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
            .setVersion(rs.getLong(2)).setType(rs.getString(3))
            .setStatus(rs.getString(4)).setPriority(rs.getInt(5))
            .setSubType(rs.getString(6)).setData(data)
            .setProcessingTriesCount(rs.getLong(8));
      });
      return (T) getFirst(result);
    } else if (clazz.equals(FullTaskRecord.class)) {
      List<FullTaskRecord> result = jdbcTemplate.query(getTaskSql2, args(taskId), (rs, rowNum) -> {
        byte[] data = getData(rs, 7, 12, 13);
        return new FullTaskRecord().setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
            .setVersion(rs.getLong(2)).setType(rs.getString(3))
            .setStatus(rs.getString(4)).setPriority(rs.getInt(5))
            .setSubType(rs.getString(6)).setData(data)
            .setProcessingTriesCount(rs.getLong(8))
            .setStateTime(toZonedDateTime(rs.getTimestamp(9)))
            .setNextEventTime(toZonedDateTime(rs.getTimestamp(10)))
            .setProcessingClientId(rs.getString(11));
      });
      return (T) getFirst(result);
    } else {
      throw new IllegalStateException("Unsupported class of '" + clazz.getCanonicalName() + "'.");
    }
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public DeleteFinishedOldTasksResult deleteOldTasks(TaskStatus taskStatus, Duration age, int batchSize) {
    if (tasksProperties.isParanoidTasksCleaning()) {
      return deleteOldTasksParanoid(taskStatus, age, batchSize);
    }

    DeleteFinishedOldTasksResult result = new DeleteFinishedOldTasksResult();
    Timestamp deletedBeforeTime = Timestamp.from(Instant.now(TwContextClockHolder.getClock()).minus(age));

    List<Object> taskIds = jdbcTemplate.query(deleteFinishedOldTasksSql2,
        args(taskStatus.name(), deletedBeforeTime, batchSize), (rs, rowNum) -> rs.getObject(1));

    if (!taskIds.isEmpty()) {
      UUID firstDeletedTaskId = sqlMapper.sqlTaskIdToUuid(taskIds.get(0));
      result.setFirstDeletedTaskId(firstDeletedTaskId);
      ZonedDateTime nextEventTime = getFirst(jdbcTemplate.query(deleteFinishedOldTasksSql1,
          args(firstDeletedTaskId),
          (rs, rowNum) -> TimeUtils.toZonedDateTime(rs.getTimestamp(1))));

      result.setFirstDeletedTaskNextEventTime(nextEventTime);
    }

    int deletedTasksCount = 0;
    int deletedUniqueTaskKeysCount = 0;
    int deleteTaskDatasCount = 0;
    int processedIdsCount = 0;

    for (int b = questionBuckets.length - 1; b >= 0; b--) {
      int bucketSize = questionBuckets[b];
      while (taskIds.size() - processedIdsCount >= bucketSize) {

        String tasksDeleteSql = sqlCache.computeIfAbsent(
            new CacheKey(DELETE_TASKS_BY_ID_BATCHES, b),
            k -> SqlHelper.expandParametersList(deleteTasksByIdBatchesSql, bucketSize)
        );

        final int currentProcessedIdsCount = processedIdsCount;
        deletedTasksCount += jdbcTemplate.update(con -> {
          PreparedStatement ps = con.prepareStatement(tasksDeleteSql);
          try {
            for (int i = 0; i < bucketSize; i++) {
              ps.setObject(1 + i, taskIds.get(i + currentProcessedIdsCount));
            }
          } catch (Throwable t) {
            ps.close();
            throw t;
          }
          return ps;
        });

        String uniqueTaskKeysDeleteSql = sqlCache.computeIfAbsent(
            new CacheKey(DELETE_UNIQUE_TASK_KEYS_BY_ID_BATCHES, b),
            k -> SqlHelper.expandParametersList(deleteUniqueTaskKeysByIdBatchesSql, bucketSize)
        );

        deletedUniqueTaskKeysCount += jdbcTemplate.update(con -> {
          PreparedStatement ps = con.prepareStatement(uniqueTaskKeysDeleteSql);
          try {
            for (int i = 0; i < bucketSize; i++) {
              ps.setObject(1 + i, taskIds.get(i + currentProcessedIdsCount));
            }
          } catch (Throwable t) {
            ps.close();
            throw t;
          }
          return ps;
        });

        String taskDatasDeleteSql = sqlCache.computeIfAbsent(
            new CacheKey(DELETE_TASK_DATAS_BY_ID_BATCHES, b),
            k -> SqlHelper.expandParametersList(deleteTaskDatasByIdBatchesSql, bucketSize)
        );

        deleteTaskDatasCount += jdbcTemplate.update(con -> {
          PreparedStatement ps = con.prepareStatement(taskDatasDeleteSql);
          try {
            for (int i = 0; i < bucketSize; i++) {
              ps.setObject(1 + i, taskIds.get(i + currentProcessedIdsCount));
            }
          } catch (Throwable t) {
            ps.close();
            throw t;
          }
          return ps;
        });

        processedIdsCount += bucketSize;
      }

    }

    result.setDeletedTasksCount(deletedTasksCount);
    result.setDeletedUniqueKeysCount(deletedUniqueTaskKeysCount);
    result.setDeletedTaskDatasCount(deleteTaskDatasCount);
    result.setFoundTasksCount(taskIds.size());
    result.setDeletedBeforeTime(TimeUtils.toZonedDateTime(deletedBeforeTime));

    return result;
  }

  protected DeleteFinishedOldTasksResult deleteOldTasksParanoid(TaskStatus taskStatus, Duration age, int batchSize) {
    DeleteFinishedOldTasksResult result = new DeleteFinishedOldTasksResult();
    Timestamp deletedBeforeTime = Timestamp.from(Instant.now(TwContextClockHolder.getClock()).minus(age));

    List<Pair<Object, Long>> taskVersionIds = jdbcTemplate.query(deleteFinishedOldTasksSql,
        args(taskStatus.name(), deletedBeforeTime, batchSize),
        (rs, rowNum) -> ImmutablePair.of(rs.getObject(1), rs.getLong(2)));

    if (!taskVersionIds.isEmpty()) {
      UUID firstDeletedTaskId = sqlMapper.sqlTaskIdToUuid(taskVersionIds.get(0).getLeft());
      result.setFirstDeletedTaskId(firstDeletedTaskId);
      ZonedDateTime nextEventTime = getFirst(jdbcTemplate.query(deleteFinishedOldTasksSql1,
          args(firstDeletedTaskId),
          (rs, rowNum) -> TimeUtils.toZonedDateTime(rs.getTimestamp(1))));

      result.setFirstDeletedTaskNextEventTime(nextEventTime);
    }

    int tasksCount = 0;
    int[] tasksDeleteResult = jdbcTemplate.batchUpdate(deleteTaskSql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setObject(1, taskVersionIds.get(i).getLeft());
        ps.setLong(2, taskVersionIds.get(i).getRight());
      }

      @Override
      public int getBatchSize() {
        return taskVersionIds.size();
      }
    });

    List<Object> deletedIds = new ArrayList<>();
    for (int i = 0; i < tasksDeleteResult.length; i++) {
      if (tasksDeleteResult[i] > 0) {
        deletedIds.add(taskVersionIds.get(i).getLeft());
        tasksCount += tasksDeleteResult[i];
      }
    }

    int uniqueTaskKeysCount = Arrays.stream(jdbcTemplate.batchUpdate(deleteUniqueTaskKeySql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setObject(1, deletedIds.get(i));
      }

      @Override
      public int getBatchSize() {
        return deletedIds.size();
      }
    })).sum();

    int taskDatasCount = Arrays.stream(jdbcTemplate.batchUpdate(deleteTaskDataSql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setObject(1, deletedIds.get(i));
      }

      @Override
      public int getBatchSize() {
        return deletedIds.size();
      }
    })).sum();

    result.setDeletedTasksCount(result.getDeletedTasksCount() + tasksCount);
    result.setDeletedUniqueKeysCount(result.getDeletedUniqueKeysCount() + uniqueTaskKeysCount);
    result.setDeletedTaskDatasCount(result.getDeletedTaskDatasCount() + taskDatasCount);
    result.setFoundTasksCount(result.getFoundTasksCount() + taskVersionIds.size());
    result.setDeletedBeforeTime(TimeUtils.toZonedDateTime(deletedBeforeTime));

    return result;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean deleteTask(UUID taskId, long version) {
    int updatedCount = jdbcTemplate.update(deleteTaskSql, args(taskId, version));
    if (updatedCount != 0) {
      jdbcTemplate.update(deleteUniqueTaskKeySql, args(taskId));
      jdbcTemplate.update(deleteTaskDataSql, args(taskId));
    }
    return updatedCount > 0;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean clearPayloadAndMarkDone(UUID taskId, long version) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    int updatedCount = jdbcTemplate.update(clearPayloadAndMarkDoneSql, args(TaskStatus.DONE, now, now, version + 1,
        taskId, version));

    if (updatedCount == 1) {
      jdbcTemplate.update(deleteTaskDataSql, args(taskId));
    }

    return updatedCount == 1;
  }

  @Override
  public Long getTaskVersion(UUID taskId) {
    return getFirst(jdbcTemplate.query(getTaskVersionSql, args(taskId), (rs, rowNum) -> rs.getLong(1)));
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public long getApproximateTasksCount() {
    assertIsolationLevel(Isolation.READ_UNCOMMITTED);

    String sql = StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName()) ? getApproximateTasksCountSql1 : getApproximateTasksCountSql;
    List<Long> rows = jdbcTemplate.queryForList(sql, Long.class);
    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public long getApproximateUniqueKeysCount() {
    assertIsolationLevel(Isolation.READ_UNCOMMITTED);

    String sql =
        StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName()) ? getApproximateUniqueKeysCountSql1 : getApproximateUniqueKeysCountSql;
    List<Long> rows = jdbcTemplate.queryForList(sql, Long.class);
    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  @Transactional(rollbackFor = Exception.class, isolation = Isolation.READ_UNCOMMITTED)
  @MonitoringQuery
  public long getApproximateTaskDatasCount() {
    assertIsolationLevel(Isolation.READ_UNCOMMITTED);

    String sql =
        StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName()) ? getApproximateTaskDatasCountSql1 : getApproximateTaskDatasCountSql;
    List<Long> rows = jdbcTemplate.queryForList(sql, Long.class);
    return rows.isEmpty() ? -1 : rows.get(0);
  }

  //////////////////////////

  protected <T> T getFirst(List<T> list) {
    return CollectionUtils.isEmpty(list) ? null : list.get(0);
  }

  /**
   * For safety reasons, we expect warnings to be present as well.
   *
   * <p>If someone really needs to disable warnings on JDBC driver level, we can provide a configuration option for that.
   *
   * <p>1062 is both for primary and unique key failures. Tested on MariaDb and MySql.
   */
  protected boolean didInsertFailDueToDuplicateKeyConflict(SQLWarning warnings) {
    return warnings != null && warnings.getErrorCode() == 1062;
  }

  protected PreparedStatementSetter args(Object... args) {
    return new ArgumentPreparedStatementSetter(sqlMapper::uuidToSqlTaskId, args);
  }

  /**
   * Asserts isolation level.
   *
   * <p>{@link org.springframework.transaction.support.AbstractPlatformTransactionManager#validateExistingTransaction} is not usually enabled.
   * So we do our own validation, at least when running tests.
   */
  @SuppressWarnings({"SameParameterValue", "JavadocReference"})
  protected void assertIsolationLevel(Isolation isolation) {
    if (tasksProperties.isAssertionsEnabled()) {
      Connection con = DataSourceUtils.getConnection(dataSource);
      try {
        int conIsolation = ExceptionUtils.doUnchecked(con::getTransactionIsolation);
        Preconditions.checkState(isolation.value() == conIsolation, "Connection isolation does not match. %s != %s", isolation, conIsolation);
      } finally {
        DataSourceUtils.releaseConnection(con, dataSource);
      }
    }
  }

  protected byte[] getData(ResultSet rs, int deprecatedDataIdx, int dataFormatIdx, int dataIdx) throws SQLException {
    byte[] data = rs.getBytes(dataIdx);
    if (data != null) {
      return taskDataSerializer.deserialize(new SerializedData().setDataFormat(rs.getInt(dataFormatIdx)).setData(data));
    } else {
      String deprecatedData = rs.getString(deprecatedDataIdx);
      return StringUtils.isEmpty(deprecatedData) ? null : deprecatedData.getBytes(StandardCharsets.UTF_8);
    }
  }

  protected String getApproximateTableCountSql(boolean withSchema, String table) {
    String schema = "DATABASE()";
    if (!withSchema) {
      schema = "'" + tasksProperties.getTaskTablesSchemaName() + "'";
    }
    return "select table_rows from information_schema.tables where table_schema=" + schema + " and table_name = '" + table + "'";
  }
}
