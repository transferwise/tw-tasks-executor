package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.PostgresTaskSqlMapper;
import com.transferwise.tasks.dao.PostgresTaskTables;
import javax.sql.DataSource;

public class PostgresTestTaskDao extends JdbcTestTaskDao {

  public PostgresTestTaskDao(DataSource dataSource, TasksProperties properties) {
    super(dataSource, new PostgresTaskTables(properties), new PostgresTaskSqlMapper());
  }
}
