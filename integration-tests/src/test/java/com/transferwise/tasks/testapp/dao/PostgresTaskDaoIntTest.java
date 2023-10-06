package com.transferwise.tasks.testapp.dao;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.common.spyql.SpyqlDataSource;
import com.transferwise.common.spyql.event.StatementExecuteEvent;
import com.transferwise.common.spyql.listener.SpyqlConnectionListener;
import com.transferwise.common.spyql.listener.SpyqlDataSourceListener;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("postgres")
class PostgresTaskDaoIntTest extends TaskDaoIntTest {

  @Test
  @SneakyThrows
  void approximateTaskCountCanBeRetrieved() {
    var correctSqlEncountered = new AtomicBoolean();
    var spyqlDataSource = dataSource.unwrap(SpyqlDataSource.class);
    final SpyqlDataSourceListener spyqlDataSourceListener = event -> new SpyqlConnectionListener() {
      @Override
      public void onStatementExecute(StatementExecuteEvent event) {
        // Check if correct schema is set.
        if (event.getSql().equals(
            "SELECT reltuples as approximate_row_count FROM pg_class, pg_namespace WHERE "
                + " pg_class.relnamespace=pg_namespace.oid and nspname='public' and relname = 'tw_task'")) {
          correctSqlEncountered.set(true);
        }
      }
    };

    spyqlDataSource.addListener(spyqlDataSourceListener);

    try {
      assertThat(taskDao.getApproximateTasksCount()).isGreaterThan(-1);
      assertThat(correctSqlEncountered).isTrue();
    } finally {
      spyqlDataSource.getDataSourceListeners().remove(spyqlDataSourceListener);
    }
  }
}
