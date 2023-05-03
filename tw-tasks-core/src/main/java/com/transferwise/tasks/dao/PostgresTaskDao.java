package com.transferwise.tasks.dao;

import com.transferwise.tasks.TasksProperties;
import java.sql.SQLWarning;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

public class PostgresTaskDao extends JdbcTaskDao {

  public PostgresTaskDao(DataSource dataSource) {
    super(dataSource, new PostgresTaskSqlMapper());
  }

  @Override
  protected ITwTaskTables twTaskTables(TasksProperties tasksProperties) {
    return new PostgresTaskTables(tasksProperties);
  }

  @PostConstruct
  @Override
  public void init() {
    super.init();

    ITwTaskTables tables = twTaskTables(tasksProperties);
    String taskTable = tables.getTaskTableIdentifier();
    String uniqueTaskKeyTable = tables.getUniqueTaskKeyTableIdentifier();

    insertTaskSql = "insert into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values"
        + "(?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing";
    insertUniqueTaskKeySql = "insert into " + uniqueTaskKeyTable + "(task_id,key_hash,key) values"
        + "(?, ?, ?) on conflict (key_hash, key) do nothing";

    lockTasksForDeleteBatchesSql = "select id, version from " + taskTable + " where id in (??) for update skip locked";
  }

  @Override
  protected boolean didInsertFailDueToDuplicateKeyConflict(SQLWarning warnings) {
    return warnings == null;
  }

  @Override
  protected String getApproximateTableCountSql(boolean withSchema, String table) {
    String schema = "current_schema()";
    if (!withSchema) {
      schema = "'" + tasksProperties.getTaskTablesSchemaName() + "'";
    }

    return "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + " pg_class.relnamespace=pg_namespace.oid and nspname=" + schema + " and relname = '" + table + "'";
  }
}
