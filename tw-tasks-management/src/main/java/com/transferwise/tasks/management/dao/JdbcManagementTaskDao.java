package com.transferwise.tasks.management.dao;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.dao.ITaskSqlMapper;
import com.transferwise.tasks.dao.ITwTaskTables;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.sql.ArgumentPreparedStatementSetter;
import com.transferwise.tasks.helpers.sql.CacheKey;
import com.transferwise.tasks.helpers.sql.SqlHelper;
import com.transferwise.tasks.utils.TimeUtils;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;

public class JdbcManagementTaskDao implements IManagementTaskDao {

  private static class Queries {

    static final String GET_TASKS = "getTasks";

    final String scheduleTaskForImmediateExecution;
    final String getTasksInErrorStatus;
    final String getStuckTasks;
    final String getTasksInStatus;
    final String getTasks;

    Queries(ITwTaskTables tables) {
      scheduleTaskForImmediateExecution = "update " + tables.getTaskTableIdentifier() + " set status=?"
          + ",next_event_time=?,state_time=?,time_updated=?,version=? where id=? and version=?";
      getTasksInErrorStatus = "select id,version,state_time,type,sub_type from " + tables.getTaskTableIdentifier()
          + " where status='" + TaskStatus.ERROR.name() + "' order by next_event_time desc limit ?";
      getStuckTasks = "select id,version,next_event_time from " + tables.getTaskTableIdentifier() + " where status=?"
          + " and next_event_time<? order by next_event_time desc limit ?";
      getTasksInStatus = "select id,version,state_time,type,sub_type,status,next_event_time from " + tables.getTaskTableIdentifier()
          + " where status in (?) order by next_event_time desc limit ?";
      getTasks = "select id,type,sub_type,data,status,version,processing_tries_count,priority,state_time"
          + ",next_event_time,processing_client_id from " + tables.getTaskTableIdentifier() + " where id in (??)";
    }
  }

  private static final TaskStatus[] STUCK_STATUSES = new TaskStatus[]{
      TaskStatus.NEW,
      TaskStatus.SUBMITTED,
      TaskStatus.WAITING,
      TaskStatus.PROCESSING
  };

  private static final TaskStatus[] WAITING_AND_PROCESSING_STATUSES = new TaskStatus[]{
      TaskStatus.WAITING,
      TaskStatus.PROCESSING
  };

  protected final int[] questionBuckets = {1, 5, 25, 125, 625};

  private final JdbcTemplate jdbcTemplate;
  private final Queries queries;
  private final ITaskSqlMapper sqlMapper;
  private final ConcurrentHashMap<CacheKey, String> queriesCache;

  public JdbcManagementTaskDao(DataSource dataSource, ITwTaskTables tables, ITaskSqlMapper sqlMapper) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.queries = new Queries(tables);
    this.sqlMapper = sqlMapper;
    this.queriesCache = new ConcurrentHashMap<>();
  }

  @Override
  public List<DaoTask1> getTasksInErrorStatus(int maxCount) {
    return jdbcTemplate.query(
        queries.getTasksInErrorStatus,
        args(maxCount),
        (rs, rowNum) ->
            new DaoTask1()
                .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                .setVersion(rs.getLong(2))
                .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3)))
                .setType(rs.getString(4))
                .setSubType(rs.getString(5))
    );
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public boolean scheduleTaskForImmediateExecution(UUID taskId, long version) {
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    return jdbcTemplate.update(
        queries.scheduleTaskForImmediateExecution,
        args(TaskStatus.WAITING, now, now, now, version + 1, taskId, version)
    ) == 1;
  }

  @Override
  public List<DaoTask2> getStuckTasks(int maxCount, Duration delta) {
    Timestamp timeThreshold = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant().minus(delta));

    List<DaoTask2> stuckTasks = new ArrayList<>();
    for (TaskStatus taskStatus : STUCK_STATUSES) {
      stuckTasks.addAll(
          jdbcTemplate.query(
              queries.getStuckTasks,
              args(taskStatus, timeThreshold, maxCount),
              (rs, rowNum) ->
                  new DaoTask2()
                      .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                      .setVersion(rs.getLong(2))
                      .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3)))
          )
      );
    }

    stuckTasks.sort(Comparator.comparing(DaoTask2::getNextEventTime).reversed());

    return stuckTasks.subList(0, Math.min(maxCount, stuckTasks.size()));
  }

  @Override
  public List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount) {
    List<DaoTask3> result = new ArrayList<>();
    for (TaskStatus taskStatus : WAITING_AND_PROCESSING_STATUSES) {
      result.addAll(
          jdbcTemplate.query(
              queries.getTasksInStatus,
              args(taskStatus, maxCount),
              (rs, rowNum) ->
                  new DaoTask3()
                      .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                      .setVersion(rs.getLong(2))
                      .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(3)))
                      .setType(rs.getString(4))
                      .setSubType(rs.getString(5))
                      .setStatus(rs.getString(6))
                      .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(7)))
          )
      );
    }

    result.sort(Comparator.comparing(DaoTask3::getNextEventTime).reversed());

    return result.subList(0, Math.min(maxCount, result.size()));
  }

  @Override
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

      String sql = queriesCache.computeIfAbsent(
          new CacheKey(Queries.GET_TASKS, bucketId),
          k -> SqlHelper.expandParametersList(queries.getTasks, questionsCount)
      );

      result.addAll(
          jdbcTemplate.query(
              sql,
              args(taskIds.subList(idx, idx + questionsCount)),
              (rs, rowNum) ->
                  new FullTaskRecord()
                      .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                      .setType(rs.getString(2))
                      .setSubType(rs.getString(3))
                      .setData(rs.getString(4))
                      .setStatus(rs.getString(5))
                      .setVersion(rs.getLong(6))
                      .setProcessingTriesCount(rs.getLong(7))
                      .setPriority(rs.getInt(8))
                      .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(9)))
                      .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(10)))
          )
      );
      idx += questionsCount;
    }
  }

  protected PreparedStatementSetter args(Object... args) {
    return new ArgumentPreparedStatementSetter(sqlMapper::uuidToSqlTaskId, args);
  }
}
