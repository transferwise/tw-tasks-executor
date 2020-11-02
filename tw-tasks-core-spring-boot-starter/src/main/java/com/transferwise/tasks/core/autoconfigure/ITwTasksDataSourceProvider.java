package com.transferwise.tasks.core.autoconfigure;

import javax.sql.DataSource;

public interface ITwTasksDataSourceProvider {

  DataSource getDataSource();
}
