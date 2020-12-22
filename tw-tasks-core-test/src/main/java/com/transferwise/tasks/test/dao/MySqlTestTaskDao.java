package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer;
import com.transferwise.tasks.dao.MySqlTaskTables;
import com.transferwise.tasks.dao.MySqlTaskTypesMapper;
import javax.sql.DataSource;

public class MySqlTestTaskDao extends JdbcTestTaskDao {

  public MySqlTestTaskDao(DataSource dataSource, TasksProperties tasksProperties, ITaskDaoDataSerializer taskDataSerializer) {
    super(dataSource, new MySqlTaskTables(tasksProperties), new MySqlTaskTypesMapper(), taskDataSerializer);
  }
}
