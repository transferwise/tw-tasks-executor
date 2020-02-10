package com.transferwise.tasks.dao;

import java.util.UUID;
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

    String taskTable = tasksProperties.getTaskTableName();
    String uniqueTaskKeyTable = tasksProperties.getUniqueTaskKeyTableName();

    insertTaskSql = "insert into " + taskTable + "(id,type,sub_type,status,data,next_event_time"
        + ",state_time,time_created,time_updated,processing_tries_count,version,priority) values"
        + "(?,?,?,?,?,?,?,?,?,?,?,?) on conflict do nothing";
    insertUniqueTaskKeySql = "insert into " + uniqueTaskKeyTable + "(task_id,key_hash,key) values"
        + "(?, ?, ?) on conflict (key_hash, key) do nothing";
  }

  @Override
  protected Object asUuidArg(UUID uuid) {
    return uuid;
  }
}
