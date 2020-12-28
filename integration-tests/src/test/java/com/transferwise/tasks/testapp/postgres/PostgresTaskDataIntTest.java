package com.transferwise.tasks.testapp.postgres;

import com.transferwise.tasks.dao.PostgresTaskSqlMapper;
import com.transferwise.tasks.testapp.TaskDataIntTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("postgres")
public class PostgresTaskDataIntTest extends TaskDataIntTest {

  PostgresTaskDataIntTest() {
    taskSqlMapper = new PostgresTaskSqlMapper();
  }
}
