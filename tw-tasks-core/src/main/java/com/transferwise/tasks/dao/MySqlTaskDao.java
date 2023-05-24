package com.transferwise.tasks.dao;

import javax.sql.DataSource;

public class MySqlTaskDao extends JdbcTaskDao {

  public MySqlTaskDao(DataSource dataSource) {
    super(dataSource);
  }
}
