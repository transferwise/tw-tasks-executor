package com.transferwise.tasks.dao;

import com.transferwise.tasks.TasksProperties;
import org.apache.commons.lang3.StringUtils;

public class MySqlTaskTables implements ITwTaskTables {

  private final String taskTableIdentifier;
  private final String uniqueTaskKeyTableIdentifier;

  public MySqlTaskTables(TasksProperties tasksProperties) {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      taskTableIdentifier = "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getTaskTableName() + "`";
      uniqueTaskKeyTableIdentifier = "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
    } else {
      taskTableIdentifier = "`" + tasksProperties.getTaskTableName() + "`";
      uniqueTaskKeyTableIdentifier = "`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
    }
  }

  @Override
  public String getTaskTableIdentifier() {
    return taskTableIdentifier;
  }

  @Override
  public String getUniqueTaskKeyTableIdentifier() {
    return uniqueTaskKeyTableIdentifier;
  }
}
