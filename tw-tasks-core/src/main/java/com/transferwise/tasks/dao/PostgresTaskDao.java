package com.transferwise.tasks.dao;

import java.util.UUID;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;

public class PostgresTaskDao extends MySqlTaskDao {

  public PostgresTaskDao(DataSource dataSource) {
    super(dataSource);
  }

  @PostConstruct
  @Override
  public void init() {
    super.init();

    String taskTable = getTaskTableIdentifier();
    String uniqueTaskKeyTable = getUniqieTaskKeyIdentifier();

    insertTaskSql = "insert into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values"
        + "(?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing";
    insertUniqueTaskKeySql = "insert into " + uniqueTaskKeyTable + "(task_id,key_hash,key) values"
        + "(?, ?, ?) on conflict (key_hash, key) do nothing";
    getApproximateTasksCountSql = "SELECT reltuples as approximate_row_count FROM pg_class WHERE relname = '" + taskTable + "'";
    getApproximateTasksCountSql1 = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + " pg_class.relnamespace=pg_namespace.oid and nspname='" + tasksProperties.getTaskTablesSchemaName() + "' and relname = '" + tasksProperties
        .getTaskTableName() + "'";
    getApproximateUniqueKeysCountSql = "SELECT reltuples as approximate_row_count FROM pg_class WHERE relname = '" + uniqueTaskKeyTable + "'";
    getApproximateUniqueKeysCountSql1 = "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
        + " pg_class.relnamespace=pg_namespace.oid and nspname='" + tasksProperties.getTaskTablesSchemaName() + "' and relname = '" + tasksProperties
        .getUniqueTaskKeyTableName() + "'";

  }

  @Override
  protected Object asUuidArg(UUID uuid) {
    return uuid;
  }

  protected String getTaskTableIdentifier() {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      return tasksProperties.getTaskTablesSchemaName() + "." + tasksProperties.getTaskTableName();
    }
    return tasksProperties.getTaskTableName();
  }

  protected String getUniqieTaskKeyIdentifier() {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      return tasksProperties.getTaskTablesSchemaName() + "." + tasksProperties.getUniqueTaskKeyTableName();
    }
    return tasksProperties.getUniqueTaskKeyTableName();
  }

}
