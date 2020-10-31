package com.transferwise.tasks.testapp.dao;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class JdbcDaoIntTest extends TaskDaoIntTest {

  @Autowired
  private JdbcTemplate jdbcTemplate;

  @Override
  List<String> findDataByType(String taskType) {
    return jdbcTemplate.queryForList("select data from tw_task where type=? order by id", String.class, taskType);
  }

  @Override
  int getUniqueTaskKeysCount() {
    Integer cnt = jdbcTemplate.queryForObject("select count(*) from unique_tw_task_key", Integer.class);
    // Just keep the spotbugs happy.
    return cnt == null ? 0 : cnt;
  }
}
