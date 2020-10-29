package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.dao.ITaskSqlMapper;
import com.transferwise.tasks.dao.ITwTaskTables;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.sql.ArgumentPreparedStatementSetter;
import com.transferwise.tasks.helpers.sql.CacheKey;
import com.transferwise.tasks.helpers.sql.SqlHelper;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.annotation.Transactional;

public class JdbcTestTaskDao implements ITestTaskDao {

  private static class Queries {

    static final String GET_ID_AND_VERSION_BY_TYPE_AND_SUBTYPE_AND_STATUS = "getIdAndVersionFromTaskByTypeAndSubTypeAndStatus";
    static final String GET_TASKS_BY_TYPE_AND_STATUS_AND_SUB_TYPE = "getTasksByTypeAndStatusAndSubType";

    final String deleteFromTaskTable;
    final String deleteFromUniqueKeyTable;
    final String deleteTaskByIdAndVersion;
    final String deleteUniqueTaskKeyByTaskId;
    final String[] getIdAndVersionFromTaskByTypeAndSubTypeAndStatus;
    final String[] getTasksByTypeAndStatusAndSubType;

    Queries(ITwTaskTables tables) {
      String tasksTable = tables.getTaskTableIdentifier();
      String keysTable = tables.getUniqueTaskKeyTableIdentifier();
      deleteFromTaskTable = "delete from " + tasksTable;
      deleteFromUniqueKeyTable = "delete from " + keysTable;
      deleteTaskByIdAndVersion = "delete from " + tasksTable + " where id=? and version=?";
      deleteUniqueTaskKeyByTaskId = "delete from " + keysTable + " where task_id=?";
      getIdAndVersionFromTaskByTypeAndSubTypeAndStatus = new String[]{
          "select id, version from " + tasksTable + " where type=?",
          " and sub_type=?",
          " and status in (??)"
      };
      getTasksByTypeAndStatusAndSubType = new String[]{
          "select id,type,sub_type,data,status,version,processing_tries_count,priority from " + tasksTable + " where type=?",
          " and status in (??)",
          " and sub_type=?"
      };
    }
  }

  private final JdbcTemplate jdbcTemplate;
  private final Queries queries;
  private final ITaskSqlMapper sqlMapper;
  private final ConcurrentHashMap<CacheKey, String> sqlCache;

  public JdbcTestTaskDao(DataSource dataSource, ITwTaskTables tables, ITaskSqlMapper sqlMapper) {
    this.sqlCache = new ConcurrentHashMap<>();
    jdbcTemplate = new JdbcTemplate(dataSource);
    queries = new Queries(tables);
    this.sqlMapper = sqlMapper;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteTasks(String type, String subType, TaskStatus... statuses) {
    List<Object> args = new ArrayList<>();
    args.add(type);

    if (subType != null) {
      args.add(subType);
    }
    if (ArrayUtils.isNotEmpty(statuses)) {
      Collections.addAll(args, statuses);
    }

    String query = sqlCache.computeIfAbsent(
        new CacheKey(
            Queries.GET_ID_AND_VERSION_BY_TYPE_AND_SUBTYPE_AND_STATUS,
            subType == null ? 0 : 1,
            ArrayUtils.getLength(statuses)
        ),
        k -> {
          StringBuilder sb = new StringBuilder(queries.getIdAndVersionFromTaskByTypeAndSubTypeAndStatus[0]);
          if (subType != null) {
            sb.append(queries.getIdAndVersionFromTaskByTypeAndSubTypeAndStatus[1]);
          }
          if (ArrayUtils.isNotEmpty(statuses)) {
            sb.append(SqlHelper.expandParametersList(queries.getIdAndVersionFromTaskByTypeAndSubTypeAndStatus[2], statuses.length));
          }
          return sb.toString();
        }
    );
    List<Pair<Object, Long>> taskVersionIds = jdbcTemplate.query(
        query,
        args(args.toArray()),
        (rs, rowNum) -> ImmutablePair.of(rs.getObject(1), rs.getLong(2))
    );

    int[] tasksDeleteResult = jdbcTemplate.batchUpdate(queries.deleteTaskByIdAndVersion, new BatchPreparedStatementSetter() {
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

    jdbcTemplate.batchUpdate(queries.deleteUniqueTaskKeyByTaskId, new BatchPreparedStatementSetter() {
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
  public List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses) {
    List<Object> args = new ArrayList<>();
    args.add(type);
    if (ArrayUtils.isNotEmpty(statuses)) {
      args.addAll(Arrays.asList(statuses));
    }
    if (subType != null) {
      args.add(subType);
    }

    String sql = sqlCache.computeIfAbsent(
        new CacheKey(
            Queries.GET_TASKS_BY_TYPE_AND_STATUS_AND_SUB_TYPE,
            subType == null ? 0 : 1,
            ArrayUtils.getLength(statuses)
        ),
        k -> {
          StringBuilder sb = new StringBuilder(queries.getTasksByTypeAndStatusAndSubType[0]);
          if (ArrayUtils.isNotEmpty(statuses)) {
            sb.append(SqlHelper.expandParametersList(queries.getTasksByTypeAndStatusAndSubType[1], statuses.length));
          }
          if (subType != null) {
            sb.append(queries.getTasksByTypeAndStatusAndSubType[2]);
          }
          return sb.toString();
        }
    );
    return jdbcTemplate.query(
        sql,
        args(args.toArray()),
        (rs, rowNum) ->
            new Task()
                .setId(sqlMapper.sqlTaskIdToUuid(rs.getObject(1)))
                .setType(rs.getString(2))
                .setSubType(rs.getString(3))
                .setData(rs.getString(4))
                .setStatus(rs.getString(5))
                .setVersion(rs.getLong(6))
                .setProcessingTriesCount(rs.getLong(7))
                .setPriority(rs.getInt(8))
    );
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteAllTasks() {
    jdbcTemplate.update(queries.deleteFromTaskTable);
    jdbcTemplate.update(queries.deleteFromUniqueKeyTable);
  }

  private PreparedStatementSetter args(Object... args) {
    return new ArgumentPreparedStatementSetter(sqlMapper::uuidToSqlTaskId, args);
  }
}
