package com.transferwise.tasks.core.autoconfigure;

import javax.sql.DataSource;

public class TwTasksDataSourceProvider implements ITwTasksDataSourceProvider{

  private final DataSource dataSource;

  public TwTasksDataSourceProvider(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public DataSource getDataSource() {
    return dataSource;
  }
}
