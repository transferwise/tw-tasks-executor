package com.transferwise.tasks.ext.jobs;

import static org.assertj.core.api.Assertions.assertThat;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.impl.jobs.JobTaskHandler;
import com.transferwise.tasks.testapp.config.TestConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class JobTaskHandlerIntTest extends BaseIntTest {

  @Autowired
  private JobTaskHandler jobTaskHandler;

  @Test
  void handlesReturnsFalseForNonExistentJobs() {
    BaseTask task = new BaseTask().setType("TaskJob|" + TestConfiguration.TEST_JOB_UNIQUE_NAME);
    assertThat(jobTaskHandler.handles(task)).isTrue();

    task.setType("TaskJob|NonExistingJob");
    assertThat(jobTaskHandler.handles(task)).isFalse();
  }
}
