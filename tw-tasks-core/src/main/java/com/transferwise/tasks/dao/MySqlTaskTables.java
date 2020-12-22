package com.transferwise.tasks.dao;

import com.transferwise.tasks.TasksProperties;
import org.apache.commons.lang3.StringUtils;

public class MySqlTaskTables implements ITwTaskTables {

  private final String taskTableIdentifier;
  private final String uniqueTaskKeyTableIdentifier;
  private final String taskDataTableIdentifier;

  public MySqlTaskTables(TasksProperties tasksProperties) {
    if (StringUtils.isNotEmpty(tasksProperties.getTaskTablesSchemaName())) {
      taskTableIdentifier = "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getTaskTableName() + "`";
      uniqueTaskKeyTableIdentifier = "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
      taskDataTableIdentifier = "`" + tasksProperties.getTaskTablesSchemaName() + "`.`" + tasksProperties.getTaskDataTableName() + "`";
    } else {
      taskTableIdentifier = "`" + tasksProperties.getTaskTableName() + "`";
      uniqueTaskKeyTableIdentifier = "`" + tasksProperties.getUniqueTaskKeyTableName() + "`";
      taskDataTableIdentifier = "`" + tasksProperties.getTaskDataTableName() + "`";
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

  @Override
  public String getTaskDataTableIdentifier() {
    return taskDataTableIdentifier;
  }
}
