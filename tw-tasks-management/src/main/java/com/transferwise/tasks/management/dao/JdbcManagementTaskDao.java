package com.transferwise.tasks.management.dao;

import static java.util.stream.Collectors.filtering;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer.SerializedData;
import com.transferwise.tasks.dao.ITaskSqlMapper;
import com.transferwise.tasks.dao.ITwTaskTables;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.sql.ArgumentPreparedStatementSetter;
import com.transferwise.tasks.helpers.sql.CacheKey;
import com.transferwise.tasks.helpers.sql.SqlHelper;
import com.transferwise.tasks.management.dao.JdbcManagementTaskDao.Queries.QueryBuilder;
import com.transferwise.tasks.management.dao.JdbcManagementTaskDao.Queries.QueryBuilder.Op;
import com.transferwise.tasks.utils.TimeUtils;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;


public class JdbcManagementTaskDao implements IManagementTaskDao {

  static class Queries {

    static final String GET_TASKS = "getTasks";

    final String scheduleTaskForImmediateExecution;
    final String getTasksInErrorStatus;
    final String getStuckTasks;
    final String getTasksInStatus;
    final String getTasks;
    final String getTaskTypes;

    Queries(ITwTaskTables tables) {
      scheduleTaskForImmediateExecution = "update " + tables.getTaskTableIdentifier() + " set status=?"
          + ",next_event_time=?,state_time=?,time_updated=?,version=?";
      getTasksInErrorStatus = "select id,version,state_time,type,sub_type from " + tables.getTaskTableIdentifier();
      getStuckTasks = "select id,version,next_event_time from " + tables.getTaskTableIdentifier();
      getTasksInStatus = "select id,version,state_time,type,sub_type,status,next_event_time from " + tables.getTaskTableIdentifier();
      getTasks = "select id,type,sub_type,t.data,status,version,processing_tries_count,priority,state_time"
          + ",next_event_time,processing_client_id,d.data_format,d.data"
          + " from " + tables.getTaskTableIdentifier() + " t left join " + tables.getTaskDataTableIdentifier() + " d on t.id = d.task_id"
          + " where t.id in (??)";
      getTaskTypes = "select distinct type, sub_type from " + tables.getTaskTableIdentifier();
    }

    static QueryBuilder queryBuilder(String select) {
      if (select.toLowerCase().contains(" where ")) {
        throw new IllegalArgumentException("Use and() to build selection criteria");
      }
      return new QueryBuilder(select);
    }

    public static class QueryBuilder {
      private final String select;
      private String where;
      private String orderBy = "";
      private String limit = "";

      QueryBuilder and(String paramName) {
        return and(paramName, Op.EQUALS);
      }

      QueryBuilder and(String paramName, Op op) {
        this.where += " AND " + paramName;
        switch (op) {
          case IN:
            where += " IN (??)";
            break;
          case LESS_THAN:
            where += " <?";
            break;
          case EQUALS:
          default:
            where += "=?";
            break;
        }
        return this;
      }

      QueryBuilder expand(int count) {
        this.where = SqlHelper.expandParametersList(this.where, count);
        return this;
      }

      QueryBuilder desc(String orderBy) {
        this.orderBy = " order by " + orderBy + " desc";
        return this;
      }

      QueryBuilder withLimit() {
        this.limit = " limit ?";
        return this;
      }

      String build() {
        return this.select + this.where + this.orderBy + this.limit;
      }

      QueryBuilder(String select) {
        this.select = select;
        this.where = " WHERE 1=1";
      }

      enum Op {
        EQUALS,
        IN,
        LESS_THAN
      }
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
  private final ITaskDaoDataSerializer taskDataSerializer;

  public JdbcManagementTaskDao(DataSource dataSource, ITwTaskTables tables, ITaskSqlMapper sqlMapper, ITaskDaoDataSerializer taskDataSerializer) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
    this.queries = new Queries(tables);
    this.sqlMapper = sqlMapper;
    this.queriesCache = new ConcurrentHashMap<>();
    this.taskDataSerializer = taskDataSerializer;
  }

  @Override
  public List<DaoTask1> getTasksInErrorStatus(int maxCount, List<String> taskTypes, List<String> taskSubTypes) {
    QueryBuilder builder = Queries.queryBuilder(queries.getTasksInErrorStatus)
        .and("status")
        .desc("next_event_time")
        .withLimit();

    addTaskTypeArgs(builder, taskTypes, taskSubTypes);
    return jdbcTemplate.query(
        builder.build(),
        args(TaskStatus.ERROR.name(), taskTypes, taskSubTypes, maxCount),
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
    if (taskId == null) {
      throw new IllegalArgumentException("taskId may not be null");
    }
    Timestamp now = Timestamp.from(Instant.now(TwContextClockHolder.getClock()));
    String query = Queries.queryBuilder(queries.scheduleTaskForImmediateExecution)
        .and("id")
        .and("version")
        .build();
    return jdbcTemplate.update(
        query,
        args(TaskStatus.WAITING, now, now, now, version + 1, taskId, version)
    ) == 1;
  }

  @Override
  public List<DaoTask2> getStuckTasks(int maxCount, List<String> taskTypes, List<String> taskSubTypes, Duration delta) {
    Timestamp timeThreshold = Timestamp.from(ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant().minus(delta));

    List<DaoTask2> stuckTasks = new ArrayList<>();
    QueryBuilder builder = Queries.queryBuilder(queries.getStuckTasks)
        .and("status")
        .and("next_event_time", Op.LESS_THAN)
        .desc("next_event_time")
        .withLimit();
    addTaskTypeArgs(builder, taskTypes, taskSubTypes);
    String query = builder.build();
    for (TaskStatus taskStatus : STUCK_STATUSES) {
      stuckTasks.addAll(
          jdbcTemplate.query(
              query,
              args(taskStatus.name(), timeThreshold, taskTypes, taskSubTypes, maxCount),
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
  public List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount, List<String> taskTypes, List<String> taskSubTypes) {
    List<DaoTask3> result = new ArrayList<>();
    QueryBuilder builder = Queries.queryBuilder(queries.getTasksInStatus)
        .and("status")
        .desc("next_event_time")
        .withLimit();
    addTaskTypeArgs(builder, taskTypes, taskSubTypes);
    String query = builder.build();
    for (TaskStatus taskStatus : WAITING_AND_PROCESSING_STATUSES) {
      result.addAll(
          jdbcTemplate.query(
              query,
              args(taskStatus, taskTypes, taskSubTypes, maxCount),
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
              (rs, rowNum) -> {
                byte[] data;
                byte[] newData = rs.getBytes(13);
                if (newData != null) {
                  data = taskDataSerializer.deserialize(new SerializedData().setDataFormat(rs.getInt(12)).setData(newData));
                } else {
                  data = rs.getBytes(4);
                }
                return new FullTaskRecord()
                    .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                    .setType(rs.getString(2))
                    .setSubType(rs.getString(3))
                    .setData(data)
                    .setStatus(rs.getString(5))
                    .setVersion(rs.getLong(6))
                    .setProcessingTriesCount(rs.getLong(7))
                    .setPriority(rs.getInt(8))
                    .setStateTime(TimeUtils.toZonedDateTime(rs.getTimestamp(9)))
                    .setNextEventTime(TimeUtils.toZonedDateTime(rs.getTimestamp(10)))
                    .setProcessingClientId(rs.getString(11));
              }
          )
      );
      idx += questionsCount;
    }
  }

  @Override
  public List<DaoTaskType> getTaskTypes(List<String> status) {
    var builder = Queries.queryBuilder(queries.getTaskTypes);
    if (status != null && !status.isEmpty()) {
      builder.and("status", Op.IN).expand(status.size());
    }
    List<Pair<String, String>> types = jdbcTemplate.query(
          builder.build(),
          args(status),
          (rs, rowNum) -> ImmutablePair.of(rs.getString(1), rs.getString(2)));

    return types.stream()
        .collect(groupingBy(Pair::getKey, mapping(Pair::getValue, filtering(Objects::nonNull, toList()))))
        .entrySet()
        .stream()
        .map(entry -> new DaoTaskType().setType(entry.getKey()).setSubTypes(entry.getValue().stream().sorted().collect(toList())))
        .sorted(Comparator.comparing(DaoTaskType::getType))
        .collect(toList());
  }

  protected void addTaskTypeArgs(QueryBuilder builder, List<String> taskTypes, List<String> taskSubTypes) {
    if (taskTypes != null && !taskTypes.isEmpty()) {
      builder.and("type", Op.IN).expand(taskTypes.size());
    }
    if (taskSubTypes != null && !taskSubTypes.isEmpty()) {
      builder.and("sub_type", Op.IN).expand(taskSubTypes.size());
    }
  }

  protected PreparedStatementSetter args(Object... args) {
    Object[] filtered = Arrays.stream(args).filter(Objects::nonNull).toArray();
    return new ArgumentPreparedStatementSetter(sqlMapper::uuidToSqlTaskId, filtered);
  }
}
