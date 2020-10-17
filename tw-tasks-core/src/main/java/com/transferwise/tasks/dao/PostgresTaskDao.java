package com.transferwise.tasks.dao;

import java.sql.SQLWarning;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

public class PostgresTaskDao extends MySqlTaskDao {

  public PostgresTaskDao(DataSource dataSource) {
    super(dataSource);
  }

  @PostConstruct
  @Override
  public void init() {
    super.init();

    String taskTable = dbConvention.getTaskTableIdentifier();
    String uniqueTaskKeyTable = dbConvention.getUniqueTaskKeyTableIdentifier();

    insertTaskSql = "insert into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values"
        + "(?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing";
    insertUniqueTaskKeySql = "insert into " + uniqueTaskKeyTable + "(task_id,key_hash,key) values"
        + "(?, ?, ?) on conflict (key_hash, key) do nothing";
    getApproximateTasksCountSql = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + "pg_class.relnamespace=pg_namespace.oid and nspname=current_schema() and relname = '" + tasksProperties
        .getTaskTableName() + "'";
    getApproximateTasksCountSql1 = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + "pg_class.relnamespace=pg_namespace.oid and nspname='" + tasksProperties.getTaskTablesSchemaName() + "' and relname = '" + tasksProperties
        .getTaskTableName() + "'";
    getApproximateUniqueKeysCountSql = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + " pg_class.relnamespace=pg_namespace.oid and nspname=current_schema() and relname = '" + tasksProperties
        .getUniqueTaskKeyTableName() + "'";
    getApproximateUniqueKeysCountSql1 = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + " pg_class.relnamespace=pg_namespace.oid and nspname='" + tasksProperties.getTaskTablesSchemaName() + "' and relname = '" + tasksProperties
        .getUniqueTaskKeyTableName() + "'";

  }

  @Override
  protected boolean didInsertFailDueToDuplicateKeyConflict(SQLWarning warnings) {
    return warnings == null;
  }
}
