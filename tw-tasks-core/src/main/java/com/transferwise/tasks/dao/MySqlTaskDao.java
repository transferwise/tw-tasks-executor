package com.transferwise.tasks.dao;

import static com.transferwise.tasks.utils.TimeUtils.toZonedDateTime;
import static com.transferwise.tasks.utils.TwTasksUuidUtils.toUuid;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.utils.TimeUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
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

  @Autowired
  protected TasksProperties tasksProperties;

  private final JdbcTemplate jdbcTemplate;

  public MySqlTaskDao(DataSource dataSource) {
    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  private final ConcurrentHashMap<Pair<String, Integer>, String> sqlCache = new ConcurrentHashMap<>();

  protected String insertTaskSql;
  protected String insertUniqueTaskKeySql;
  protected String setToBeRetriedSql;
  protected String setToBeRetriedSql1;
  protected String grabForProcessingSql;
  protected String setStatusSql;
  protected String scheduleTaskForImmediateExecutionSql;
  protected String getStuckTasksSql;
  protected String getStuckTasksSql1;
  protected String prepareStuckOnProcessingTaskForResumingSql;
  protected String prepareStuckOnProcessingTaskForResumingSql1;
  protected String[] findTasksByTypeSubTypeAndStatusSqls;
  protected String getTasksCountInStatusSql;
  protected String getTasksCountInErrorGroupedSql;
  protected String getStuckTasksCountSql;
  protected String getTaskSql;
  protected String getTaskSql1;
  protected String getTaskSql2;
  protected String deleteAllTasksSql;
  protected String deleteAllTasksSql1;
  protected String[] deleteTasksSqls;
  protected String deleteTaskSql;
  protected String deleteUniqueTaskKeySql;
  protected String deleteFinishedOldTasksSql;
  protected String deleteFinishedOldTasksSql1;
  protected String getTasksInErrorStatusSql;
  protected String getTasksInStatusSql;
  protected String clearPayloadAndMarkDoneSql;
  protected String getTasksSql;
  protected String getEarliesTaskNextEventTimeSql;
  protected String getTaskVersionSql;
  protected String deleteTasksByIdBatchesSql;
  protected String deleteUniqueTaskKeysByIdBatchesSql;
  protected String deleteFinishedOldTasksSql2;
  protected String getApproximateTasksCountSql;
  protected String getApproximateTasksCountSql1;
  protected String getApproximateUniqueKeysCountSql;
  protected String getApproximateUniqueKeysCountSql1;

  protected final int[] questionBuckets = {1, 5, 25, 125, 625};

  protected final TaskStatus[] stuckStatuses = new TaskStatus[]{TaskStatus.NEW, TaskStatus.SUBMITTED, TaskStatus.WAITING, TaskStatus.PROCESSING};
  protected final TaskStatus[] waitingAndProcessingStatuses = new TaskStatus[]{TaskStatus.WAITING, TaskStatus.PROCESSING};

  @PostConstruct
  public void init() {
    String taskTable = getTaskTableIdentifier();
    String uniqueTaskKeyTable = getUniqieTaskKeyIdentifier();

    insertTaskSql = "insert ignore into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values (?,?,?,?,?,?,?,?,?,?,?,?)";
    insertUniqueTaskKeySql = "insert ignore into " + uniqueTaskKeyTable + "(task_id,key_hash,`key`) values (?, ?, ?)";
    setToBeRetriedSql = "update " + taskTable + " set status=?,next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    setToBeRetriedSql1 = "update " + taskTable + " set status=?,next_event_time=?"
        + ",processing_tries_count=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    grabForProcessingSql = "update " + taskTable + " set processing_client_id=?,status=?"
        + ",processing_start_time=?,next_event_time=?,processing_tries_count=processing_tries_count+1"
        + ",state_time=?,time_updated=?,version=? where id=? and version=? and status=?";
    setStatusSql = "update " + taskTable + " set status=?,next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    scheduleTaskForImmediateExecutionSql = "update " + taskTable + " set status=?"
        + ",next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    getStuckTasksSql = "select id,version,type,priority,status from " + taskTable + " where status=?"
        + " and next_event_time<? order by next_event_time limit ?";
    getStuckTasksSql1 = "select id,version,next_event_time from " + taskTable + " where status=?"
        + " and next_event_time<? order by next_event_time desc limit ?";
    prepareStuckOnProcessingTaskForResumingSql =
        "select id,version,type,priority from " + taskTable + " where status=? and next_event_time>? and processing_client_id=?";
    prepareStuckOnProcessingTaskForResumingSql1 = "update " + taskTable + " set status=?,next_event_time=?"
        + ",state_time=?,time_updated=?,version=? where id=? and version=?";
    // Tests only, not efficient.
    findTasksByTypeSubTypeAndStatusSqls = new String[]{"select id,type,sub_type,data,status,version"
        + ",processing_tries_count,priority from " + taskTable + " where type=?", " and status in (??)", " and sub_type=?"};
    getTasksCountInStatusSql = "select count(*) from (select 1 from " + taskTable + " where status = ? order by next_event_time limit ?) q";
    getTasksCountInErrorGroupedSql = "select type, count(*) from (select type from " + taskTable + " where status='"
        + TaskStatus.ERROR.name() + "' order by next_event_time limit ?) q group by type";
    getStuckTasksCountSql = "select count(*) from (select 1 from " + taskTable + " where status in (?)"
        + " and next_event_time<? order by next_event_time limit ?) q";
    getTaskSql = "select id,version,type,status,priority from " + taskTable + " where id=?";
    getTaskSql1 = "select id,version,type,status,priority,sub_type,data,processing_tries_count from " + taskTable + " where id=?";
    getTaskSql2 = "select id,version,type,status,priority,sub_type,data"
        + ",processing_tries_count,state_time,next_event_time,processing_client_id from " + taskTable + " where id=?";
    deleteAllTasksSql = "delete from " + taskTable;
    deleteAllTasksSql1 = "delete from " + uniqueTaskKeyTable;
    // Tests only, not efficient
    deleteTasksSqls = new String[]{"select id,version from " + taskTable + " where type=?", " and sub_type=?", " and status in (??)"};
    deleteTaskSql = "delete from " + taskTable + " where id=? and version=?";
    deleteUniqueTaskKeySql = "delete from " + uniqueTaskKeyTable + " where task_id=?";
    deleteTasksByIdBatchesSql = "delete from " + taskTable + " where id in (??)";
    deleteUniqueTaskKeysByIdBatchesSql = "delete from " + uniqueTaskKeyTable + " where task_id in (??)";
    deleteFinishedOldTasksSql = "select id,version from " + taskTable + " where status=? and next_event_time<? order by next_event_time limit ?";
    deleteFinishedOldTasksSql1 = "select next_event_time from " + taskTable + " where id=?";
    deleteFinishedOldTasksSql2 = "select id from " + taskTable + " where status=? and next_event_time<? order by next_event_time limit ?";
    getTasksInErrorStatusSql = "select id,version,state_time,type,sub_type from " + taskTable
        + " where status='" + TaskStatus.ERROR.name() + "' order by next_event_time desc limit ?";
    getTasksInStatusSql = "select id,version,state_time,type,sub_type,status,next_event_time from " + taskTable
        + " where status in (?) order by next_event_time desc limit ?";
    clearPayloadAndMarkDoneSql = "update " + taskTable + " set data='',status=?,state_time=?,time_updated=?,version=? where id=? and version=?";
    getTasksSql = "select id,type,sub_type,data,status,version,processing_tries_count,priority,state_time"
        + ",next_event_time,processing_client_id from " + taskTable + " where id in (??)";
    getEarliesTaskNextEventTimeSql = "select min(next_event_time) from " + taskTable + " where status=?";
    getTaskVersionSql = "select version from " + taskTable + " where id=?";
    getApproximateTasksCountSql = "select table_rows from information_schema.tables where table_name = '" + taskTable + "'";
    getApproximateTasksCountSql1 = "select table_rows from information_schema.tables where table_schema ='"
        + tasksProperties.getTaskTablesSchemaName() + "' and table_name = '" + tasksProperties.getTaskTableName() + "'";
    getApproximateUniqueKeysCountSql = "select table_rows from information_schema.tables where table_name = '" + uniqueTaskKeyTable + "'";
    getApproximateUniqueKeysCountSql1 = "select table_rows from information_schema.tables where table_schema ='"
        + tasksProperties.getTaskTablesSchemaName() + "' and table_name = '" + tasksProperties.getUniqueTaskKeyTableName() + "'";
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public InsertTaskResponse insertTask(InsertTaskRequest request) {
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

    int insertedCount = jdbcTemplate.update(insertTaskSql, args(taskId, request.getType(), request.getSubType(),
        request.getStatus(), request.getData(), nextEventTime, now, now, now, 0, 0, request.getPriority()));

    if (insertedCount == 0) {
      return new InsertTaskResponse().setInserted(false);
    }

    return new InsertTaskResponse().setTaskId(taskId).setInserted(true);
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
  @Transactional(rollbackFor = Exception.class)
  public boolean scheduleTaskForImmediateExecution(UUID taskId, long version) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    int updatedCount = jdbcTemplate.update(scheduleTaskForImmediateExecutionSql, args(TaskStatus.WAITING,
        now, now, now, version + 1, taskId, version));
    return updatedCount == 1;
  }

  @Override
  public GetStuckTasksResponse getStuckTasks(int batchSize, TaskStatus status) {
    Timestamp now = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant());

    List<StuckTask> stuckTasks = jdbcTemplate.query(getStuckTasksSql, args(status, now, batchSize + 1), (rs, rowNum) ->
        new StuckTask()
            .setVersionId(new TaskVersionId(toUuid(rs.getObject(1)), rs.getLong(2)))
            .setType(rs.getString(3))
            .setPriority(rs.getInt(4)).setStatus(rs.getString(5)));
    boolean hasMore = stuckTasks.size() > batchSize;
    if (hasMore) {
      stuckTasks.remove(stuckTasks.size() - 1);
    }
    return new GetStuckTasksResponse().setStuckTasks(stuckTasks).setHasMore(hasMore);
  }

  /**
   * It's more efficient to do one query per status and merge the results.
   */
  @Override
  public List<DaoTask2> getStuckTasks(int maxCount, Duration delta) {
    Timestamp timeThreshold = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant().minus(delta));

    List<DaoTask2> stuckTasks = new ArrayList<>();
    for (TaskStatus taskStatus : stuckStatuses) {
      stuckTasks.addAll(jdbcTemplate.query(getStuckTasksSql1, args(taskStatus, timeThreshold, maxCount), (rs, rowNum) -> new DaoTask2()
          .setId(toUuid(rs.getObject(1))).setVersion(rs.getLong(2))
          .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3)))));
    }

    stuckTasks.sort(Comparator.comparing(DaoTask2::getNextEventTime).reversed());

    return stuckTasks.subList(0, Math.min(maxCount, stuckTasks.size()));
  }

  @Override
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
            result.add(new StuckTask().setVersionId(new TaskVersionId(toUuid(id), version + 1))
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
  // TODO: For Tests only
  public List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses) {
    final String sql = cachedSql(sqlKey("findTasksByTypeSubTypeAndStatus", subType == null ? 0 : 1, ArrayUtils.getLength(statuses)),
        () -> {
          StringBuilder sb = new StringBuilder(findTasksByTypeSubTypeAndStatusSqls[0]);
          if (ArrayUtils.isNotEmpty(statuses)) {
            sb.append(getExpandedSql(findTasksByTypeSubTypeAndStatusSqls[1], statuses.length));
          }
          if (subType != null) {
            sb.append(findTasksByTypeSubTypeAndStatusSqls[2]);
          }
          return sb.toString();
        });
    List<Object> args = new ArrayList<>();
    args.add(type);
    if (ArrayUtils.isNotEmpty(statuses)) {
      args.addAll(Arrays.asList(statuses));
    }
    if (subType != null) {
      args.add(subType);
    }

    return jdbcTemplate.query(sql, args(args.toArray(new Object[0])), (rs, rowNum) ->
        new Task().setId(toUuid(rs.getObject(1)))
            .setType(rs.getString(2))
            .setSubType(rs.getString(3))
            .setData(rs.getString(4))
            .setStatus(rs.getString(5))
            .setVersion(rs.getLong(6))
            .setProcessingTriesCount(rs.getLong(7))
            .setPriority(rs.getInt(8)));
  }

  @Override
  public int getTasksCountInStatus(int maxCount, TaskStatus status) {
    List<Integer> results = jdbcTemplate.query(getTasksCountInStatusSql, args(status, maxCount),
        (rs, rowNum) -> rs.getInt(1));

    return DataAccessUtils.intResult(results);
  }

  @Override
  public List<Pair<String, Integer>> getTasksCountInErrorGrouped(int maxCount) {
    return jdbcTemplate.query(getTasksCountInErrorGroupedSql, args(maxCount), (rs, rowNum) ->
        new ImmutablePair<>(rs.getString(1), rs.getInt(2)));
  }

  @Override
  public int getStuckTasksCount(ZonedDateTime age, int maxCount) {
    int cnt = 0;

    for (TaskStatus taskStatus : stuckStatuses) {
      List<Integer> results = jdbcTemplate.query(getStuckTasksCountSql, args(taskStatus, age, maxCount), (rs, rowNum) -> rs.getInt(1));

      cnt += DataAccessUtils.intResult(results);
    }

    return cnt;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getTask(UUID taskId, Class<T> clazz) {
    if (taskId == null) {
      return null;
    }
    if (clazz.equals(BaseTask1.class)) {
      List<BaseTask1> result = jdbcTemplate.query(getTaskSql, args(taskId), (rs, rowNum) ->
          new BaseTask1().setId(toUuid(rs.getObject(1)))
              .setVersion(rs.getLong(2)).setType(rs.getString(3))
              .setStatus(rs.getString(4)).setPriority(rs.getInt(5)));
      return (T) getFirst(result);
    } else if (clazz.equals(Task.class)) {
      List<Task> result = jdbcTemplate.query(getTaskSql1, args(taskId), (rs, rowNum) ->
          new Task().setId(toUuid(rs.getObject(1)))
              .setVersion(rs.getLong(2)).setType(rs.getString(3))
              .setStatus(rs.getString(4)).setPriority(rs.getInt(5))
              .setSubType(rs.getString(6)).setData(rs.getString(7))
              .setProcessingTriesCount(rs.getLong(8)));
      return (T) getFirst(result);
    } else if (clazz.equals(FullTaskRecord.class)) {
      List<FullTaskRecord> result = jdbcTemplate.query(getTaskSql2, args(taskId), (rs, rowNum) ->
          new FullTaskRecord().setId(toUuid(rs.getObject(1)))
              .setVersion(rs.getLong(2)).setType(rs.getString(3))
              .setStatus(rs.getString(4)).setPriority(rs.getInt(5))
              .setSubType(rs.getString(6)).setData(rs.getString(7))
              .setProcessingTriesCount(rs.getLong(8))
              .setStateTime(toZonedDateTime(rs.getTimestamp(9)))
              .setNextEventTime(toZonedDateTime(rs.getTimestamp(10)))
              .setProcessingClientId(rs.getString(11)));
      return (T) getFirst(result);
    } else {
      throw new IllegalStateException("Unsupported class of '" + clazz.getCanonicalName() + "'.");
    }
  }

  @Override
  // For Tests only
  @Transactional(rollbackFor = Exception.class)
  public void deleteAllTasks() {
    jdbcTemplate.update(deleteAllTasksSql);
    jdbcTemplate.update(deleteAllTasksSql1);
  }

  @Override
  // For Tests only
  @Transactional(rollbackFor = Exception.class)
  public void deleteTasks(String type, String subType, TaskStatus... statuses) {
    final String sql = cachedSql(sqlKey("deleteTasksSql", subType == null ? 0 : 1, ArrayUtils.getLength(statuses)),
        () -> {
          StringBuilder sb = new StringBuilder(deleteTasksSqls[0]);
          if (subType != null) {
            sb.append(deleteTasksSqls[1]);
          }
          if (ArrayUtils.isNotEmpty(statuses)) {
            sb.append(getExpandedSql(deleteTasksSqls[2], statuses.length));
          }
          return sb.toString();
        });

    List<Object> args = new ArrayList<>();
    args.add(type);

    if (subType != null) {
      args.add(subType);
    }
    if (ArrayUtils.isNotEmpty(statuses)) {
      Collections.addAll(args, statuses);
    }
    List<Pair<Object, Long>> taskVersionIds = jdbcTemplate.query(sql,
        args(args.toArray(new Object[0])),
        (rs, rowNum) -> ImmutablePair.of(rs.getObject(1), rs.getLong(2)));

    int[] tasksDeleteResult = jdbcTemplate.batchUpdate(deleteTaskSql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setObject(1, taskVersionIds.get(i).getLeft());
        ps.setObject(2, taskVersionIds.get(i).getRight());
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
      }
    }

    jdbcTemplate.batchUpdate(deleteUniqueTaskKeySql, new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int i) throws SQLException {
        ps.setObject(1, deletedIds.get(i));
      }

      @Override
      public int getBatchSize() {
        return deletedIds.size();
      }
    });
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
      UUID firstDeletedTaskId = toUuid(taskIds.get(0));
      result.setFirstDeletedTaskId(firstDeletedTaskId);
      ZonedDateTime nextEventTime = getFirst(jdbcTemplate.query(deleteFinishedOldTasksSql1,
          args(firstDeletedTaskId),
          (rs, rowNum) -> TimeUtils.toZonedDateTime(rs.getTimestamp(1))));

      result.setFirstDeletedTaskNextEventTime(nextEventTime);
    }

    int deletedTasksCount = 0;
    int deletedUniqueTaskKeysCount = 0;
    int processedIdsCount = 0;

    for (int b = questionBuckets.length - 1; b >= 0; b--) {
      int bucketSize = questionBuckets[b];
      while (taskIds.size() - processedIdsCount >= bucketSize) {

        String tasksDeleteSql = cachedSql(sqlKey("deleteTasksByIdBatchesSql", bucketSize),
            () -> getExpandedSql(deleteTasksByIdBatchesSql, bucketSize));

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

        String uniqueTaskKeysDeleteSql = cachedSql(sqlKey("deleteUniqueTaskKeysByIdBatchesSql", bucketSize),
            () -> getExpandedSql(deleteUniqueTaskKeysByIdBatchesSql, bucketSize));
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

        processedIdsCount += bucketSize;
      }
    }

    result.setDeletedTasksCount(deletedTasksCount);
    result.setDeletedUniqueKeysCount(deletedUniqueTaskKeysCount);
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
      UUID firstDeletedTaskId = toUuid(taskVersionIds.get(0).getLeft());
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

    result.setDeletedTasksCount(result.getDeletedTasksCount() + tasksCount);
    result.setDeletedUniqueKeysCount(result.getDeletedUniqueKeysCount() + uniqueTaskKeysCount);
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
    }
    return updatedCount > 0;
  }

  @Override
  public List<DaoTask1> getTasksInErrorStatus(int maxCount) {
    return jdbcTemplate.query(getTasksInErrorStatusSql, args(maxCount),
        (rs, rowNum) -> new DaoTask1().setId(toUuid(rs.getObject(1)))
            .setVersion(rs.getLong(2))
            .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3))).setType(rs.getString(4))
            .setSubType(rs.getString(5)));
  }

  /**
   * It's more efficient to do a one query per status and merge the results.
   */
  @Override
  public List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount) {
    List<DaoTask3> result = new ArrayList<>();
    for (TaskStatus taskStatus : waitingAndProcessingStatuses) {
      result.addAll(jdbcTemplate.query(getTasksInStatusSql, args(taskStatus, maxCount),
          (rs, rowNum) -> new DaoTask3().setId(toUuid(rs.getObject(1)))
              .setVersion(rs.getLong(2))
              .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3))).setType(rs.getString(4))
              .setSubType(rs.getString(5)).setStatus(rs.getString(6))
              .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(7)))));
    }

    result.sort(Comparator.comparing(DaoTask3::getNextEventTime).reversed());

    return result.subList(0, Math.min(maxCount, result.size()));
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean clearPayloadAndMarkDone(UUID taskId, long version) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    int updatedCount = jdbcTemplate.update(clearPayloadAndMarkDoneSql, args(TaskStatus.DONE, now, now, version + 1,
        taskId, version));

    return updatedCount == 1;
  }

  @Override
  public Long getTaskVersion(UUID taskId) {
    return getFirst(jdbcTemplate.query(getTaskVersionSql, args(taskId), (rs, rowNum) -> rs.getLong(1)));
  }

  @Override
  //TODO: Management only
  public List<FullTaskRecord> getTasks(List<UUID> taskIds) {
    List<FullTaskRecord> result = new ArrayList<>();

    int idx = 0;
    while (true) {
      int idsLeft = taskIds.size() - idx;
      if (idsLeft < 1) {
        return result;
      }
      int bucketId = 0;
      for (int j = questionBuckets.length - 1; j >= 0; j--) {
        if (questionBuckets[j] <= idsLeft) {
          bucketId = j;
          break;
        }
      }
      int questionsCount = questionBuckets[bucketId];

      String sql = cachedSql(sqlKey("getTasks", bucketId), () ->
          getExpandedSql(getTasksSql, questionsCount));

      result.addAll(jdbcTemplate.query(sql, args(taskIds.subList(idx, idx + questionsCount)),
          ((rs, rowNum) ->
              new FullTaskRecord().setId(toUuid(rs.getObject(1))).setType(rs.getString(2))
                  .setSubType(rs.getString(3)).setData(rs.getString(4))
                  .setStatus(rs.getString(5)).setVersion(rs.getLong(6))
                  .setProcessingTriesCount(rs.getLong(7)).setPriority(rs.getInt(8))
                  .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(9)))
                  .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(10)))
          )));
      idx += questionsCount;
    }
  }

  @Override
  public long getApproximateTasksCount() {
    String sql = StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName()) ? getApproximateTasksCountSql1 : getApproximateTasksCountSql;
    List<Long> rows = jdbcTemplate.queryForList(sql, Long.class);
    return rows.isEmpty() ? -1 : rows.get(0);
  }

  @Override
  public long getApproximateUniqueKeysCount() {
    String sql =
        StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName()) ? getApproximateUniqueKeysCountSql1 : getApproximateUniqueKeysCountSql;
    List<Long> rows = jdbcTemplate.queryForList(sql, Long.class);
    return rows.isEmpty() ? -1 : rows.get(0);
  }

  //////////////////////////

  protected <T> T getFirst(List<T> list) {
    return CollectionUtils.isEmpty(list) ? null : list.get(0);
  }

  protected String getExpandedSql(String sql, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append("?");
      if (i + 1 < count) {
        sb.append(",");
      }
    }

    return sql.replace("??", sb.toString());
  }

  protected String cachedSql(Pair<String, Integer> key, Supplier<String> supplier) {
    return sqlCache.computeIfAbsent(key, ignored -> supplier.get());
  }

  // Assumes there are not more than 8 different values in each weight.
  protected Pair<String, Integer> sqlKey(String key, int... weights) {
    int sum = 1;
    for (int weight : weights) {
      sum = (sum << 3) + weight;
    }
    return ImmutablePair.of(key, sum);
  }

  protected PreparedStatementSetter args(Object... args) {
    return new ArgumentPreparedStatementSetter(args);
  }

  protected Object asUuidArg(UUID arg) {
    return UuidUtils.toBytes(arg);
  }

  protected String getTaskTableIdentifier() {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      return "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getTaskTableName() + "`";
    }
    return "`" + tasksProperties.getTaskTableName() + "`";
  }

  protected String getUniqieTaskKeyIdentifier() {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      return "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
    }
    return "`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
  }

  protected class ArgumentPreparedStatementSetter implements PreparedStatementSetter {

    private final Object[] args;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ArgumentPreparedStatementSetter(Object[] args) {
      this.args = args;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setValues(PreparedStatement ps) throws SQLException {
      int idx = 0;
      for (Object arg : args) {
        if (arg instanceof Object[]) {
          Object[] subArgs = (Object[]) arg;
          for (Object subArg : subArgs) {
            doSetValue(ps, ++idx, subArg);
          }
        } else if (arg instanceof List) {
          List<Object> subArgs = (List<Object>) arg;
          for (Object subArg : subArgs) {
            doSetValue(ps, ++idx, subArg);
          }
        } else {
          doSetValue(ps, ++idx, arg);
        }
      }
    }

    protected void doSetValue(PreparedStatement ps, int parameterPosition, Object argValue) throws SQLException {
      if (argValue instanceof SqlParameterValue) {
        SqlParameterValue paramValue = (SqlParameterValue) argValue;
        StatementCreatorUtils.setParameterValue(ps, parameterPosition, paramValue, paramValue.getValue());
      } else {
        if (argValue instanceof UUID) {
          argValue = asUuidArg((UUID) argValue);
        } else if (argValue instanceof Instant) {
          argValue = Timestamp.from((Instant) argValue);
        } else if (argValue instanceof TemporalAccessor) {
          argValue = Timestamp.from(Instant.from((TemporalAccessor) argValue));
        } else if (argValue instanceof Enum<?>) {
          argValue = ((Enum<?>) argValue).name();
        }
        StatementCreatorUtils.setParameterValue(ps, parameterPosition, SqlTypeValue.TYPE_UNKNOWN, argValue);
      }
    }
  }
}
