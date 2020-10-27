package com.transferwise.tasks.dao;

import com.transferwise.tasks.TasksProperties;
import org.apache.commons.lang3.StringUtils;

public class PostgresTaskTables implements ITwTaskTables {

  private final String taskTableIdentifier;
  private final String uniqueTaskKeyIdentifier;

  public PostgresTaskTables(TasksProperties tasksProperties) {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      taskTableIdentifier = tasksProperties.getTaskTablesSchemaName() + "." + tasksProperties.getTaskTableName();
      uniqueTaskKeyIdentifier = tasksProperties.getTaskTablesSchemaName() + "." + tasksProperties.getUniqueTaskKeyTableName();
    } else {
      taskTableIdentifier = tasksProperties.getTaskTableName();
      uniqueTaskKeyIdentifier = tasksProperties.getUniqueTaskKeyTableName();
    }
  }

  @Override
  public String getTaskTableIdentifier() {
    return taskTableIdentifier;
  }

  @Override
  public String getUniqueTaskKeyTableIdentifier() {
    return uniqueTaskKeyIdentifier;
  }
}
