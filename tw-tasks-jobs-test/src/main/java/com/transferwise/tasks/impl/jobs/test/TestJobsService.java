package com.transferwise.tasks.impl.jobs.test;

import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.impl.jobs.JobsService;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.test.ITestTasksService;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class TestJobsService extends JobsService implements ITestJobsService {

  @Autowired
  private ITransactionsHelper transactionsHelper;

  @Autowired
  private ITaskDao taskDao;

  @Autowired
  private ITestTasksService testTasksService;

  //TODO: Could refactor to Future instead.
  @Override
  public ExecuteAsyncHandle executeAsync(IJob job) {
    UUID taskId = job.getTaskId();
    long taskVersion = taskDao.getTaskVersion(taskId);

    log.info("Resuming job '{}' with task '{}'.", job.getUniqueName(), new TaskVersionId().setId(taskId).setVersion(taskVersion));

    transactionsHelper.withTransaction().asNew().call(() -> {
      boolean resumed = testTasksService.resumeTask(new ITasksService.ResumeTaskRequest()
          .setTaskId(taskId)
          .setVersion(taskVersion)
          .setForce(true));
      if (!resumed) {
        log.warn("Resuming job '{}' with task '{}' failed.", job.getUniqueName(), new TaskVersionId().setId(taskId).setVersion(taskVersion));
      }
      return null;
    });

    return new ExecuteAsyncHandle().setTaskVersionId(new TaskVersionId().setId(taskId).setVersion(taskVersion));
  }

  @Override
  public boolean hasFinished(ExecuteAsyncHandle handle) {
    FullTaskRecord task = taskDao.getTask(handle.getTaskVersionId().getId(), FullTaskRecord.class);
    return task.getVersion() > handle.getTaskVersionId().getVersion()
        && (TaskStatus.ERROR.name().equals(task.getStatus()) || TaskStatus.FAILED.name().equals(task.getStatus())
        || TaskStatus.WAITING.name().equals(task.getStatus()) || TaskStatus.DONE.name().equals(task.getStatus()));
  }

  @Override
  public void reset() {
    transactionsHelper.withTransaction().asNew().call(() -> {
      testTasksService.reset();
      if (jobsProperties.isAutoStartProcessing()) {
        initJobs(true);
      }
      return null;
    });
  }
}
