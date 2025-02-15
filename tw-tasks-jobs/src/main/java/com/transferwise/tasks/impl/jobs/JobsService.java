package com.transferwise.tasks.impl.jobs;

import com.google.common.base.Joiner;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.IBaseTask;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJobsService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@Slf4j
public class JobsService implements IJobsService, GracefulShutdownStrategy, InitializingBean {

  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private ITasksService tasksService;
  @Autowired
  protected JobsProperties jobsProperties;
  @Autowired
  private ApplicationContext applicationContext;
  @Autowired(required = false)
  private MeterRegistry meterRegistry;

  private final List<JobContainer> jobContainers = new ArrayList<>();
  private final Map<String, JobContainer> jobContainersMap = new HashMap<>();
  private final List<IJob> nonBeanJobs = new ArrayList<>();

  public void register(IJob job) {
    nonBeanJobs.add(job);
  }

  @Override
  public void afterPropertiesSet() {
    if (meterRegistry != null) {
      // Don't need concurrent list or anything here until we don't modify it's contents.
      Gauge.builder("twTasks.jobs.registrationsCount", null, (n) -> jobContainers.size()).register(meterRegistry);
    }
  }

  @Override
  public void applicationStarted() {
    if (jobsProperties.isAutoInitialize()) {
      initJobs(false);
    }
  }

  @Override
  public boolean canShutdown() {
    return true;
  }

  @Override
  public IJob getJobFor(IBaseTask task) {
    String jobName = StringUtils.substringAfter(task.getType(), "|");
    jobName = StringUtils.substringBefore(jobName, "|");
    JobContainer jobContainer = jobContainersMap.get(jobName);
    if (jobContainer == null) {
      return null;
    }
    return jobContainer.job;
  }

  protected void initJobs(boolean silent, Collection<IJob> jobs) {
    jobContainers.clear();
    jobContainersMap.clear();
    for (IJob job : jobs) {
      var jobContainer = new JobContainer()
          .setJob(job)
          .setTaskId(job.getTaskId())
          .setUniqueName(StringUtils.trimToNull(job.getUniqueName()));
      jobContainers.add(jobContainer);
      jobContainersMap.put(jobContainer.getUniqueName(), jobContainer);
    }
    validateState();
    registerCronTasks(silent);
  }

  protected void initJobs(boolean silent) {
    List<IJob> jobs = new ArrayList<>(applicationContext.getBeansOfType(IJob.class).values());
    jobs.addAll(nonBeanJobs);
    initJobs(silent, jobs);
  }

  private void validateState() {
    jobContainers.forEach(c -> {
      if (c.getUniqueName() == null) {
        throw new IllegalStateException("Job " + c.job + " has no unique name.");
      }
      if (c.getUniqueName().contains("|")) {
        throw new IllegalStateException("Job " + c.job + " unique name can not contain '|'.");
      }
    });

    jobContainers.stream().collect(Collectors.groupingBy(JobContainer::getUniqueName, Collectors.toList())).forEach((k, v) -> {
      if (v.size() > 1) {
        throw new IllegalStateException("More than one Jobs are registered under name of '" + k + "': "
            + Joiner.on(',').join(v.stream().map(JobContainer::getJob).collect(Collectors.toList())) + ".");
      }
    });
    jobContainers.stream().collect(Collectors.groupingBy(c -> c.getJob().getTaskId(), Collectors.toList())).forEach((k, v) -> {
      if (v.size() > 1) {
        throw new IllegalStateException("More than one Jobs are using same taskId of  '" + k + "': "
            + Joiner.on(',').join(v.stream().map(JobContainer::getJob).collect(Collectors.toList())) + " .");
      }
    });
  }

  protected void registerCronTasks(boolean silent) {
    for (JobContainer jobContainer : jobContainers) {
      IJob cronTask = jobContainer.getJob();
      ZonedDateTime nextRuntime = cronTask.getNextRunTime();
      if (nextRuntime == null) {
        if (silent) {
          log.debug("Job '{}' did not provide next runtime. It will not be registered and processed.", cronTask.getUniqueName());
        } else {
          log.error("Job '{}' did not provide next runtime. It will not be registered and processed.", cronTask.getUniqueName());
        }
        continue;
      }

      String type = jobsProperties.getTaskTypePrefix() + "|" + jobContainer.getUniqueName();
      String typeSuffix = jobContainer.job.getTaskTypeSuffix();
      if (typeSuffix != null) {
        type = type + "|" + typeSuffix;
      }

      ITasksService.AddTaskResponse response = tasksService.addTask(new ITasksService.AddTaskRequest()
          .setTaskId(jobContainer.getTaskId())
          .setRunAfterTime(nextRuntime)
          .setType(type)
          .setWarnWhenTaskExists(false)
          .setData(cronTask.getInitialData()));

      if (response.getResult() == ITasksService.AddTaskResponse.Result.OK) {
        if (silent) {
          log.debug("Job '{}' registered with task id '{}'. It will be run at {}.", jobContainer.getUniqueName(), cronTask.getTaskId(), nextRuntime);
        } else {
          log.info("Job '{}' registered with task id '{}'. It will be run at {}.", jobContainer.getUniqueName(), cronTask.getTaskId(), nextRuntime);
        }
      } else {
        FullTaskRecord alreadyScheduledTask = taskDao.getTask(cronTask.getTaskId(), FullTaskRecord.class);

        if (alreadyScheduledTask.getStatus().equals(TaskStatus.ERROR.name()) && !silent) {
          log.error("Job '{}' was not registered with task id '{}', because the task already exists and is in ERROR state.",
              jobContainer.getUniqueName(),
              cronTask.getTaskId());
        } else if (jobsProperties.isTestMode() || silent) {
          // We don't want to see this every time a new test runs while tasks are not cleaned.
          log.debug("Job '{}' was not registered with task id '{}', because the task already exists.", jobContainer.getUniqueName(),
              cronTask.getTaskId());
        } else {
          log.info("Job '{}' was not registered with task id '{}', because the task already exists.", jobContainer.getUniqueName(),
              cronTask.getTaskId());
        }
      }
    }
  }

  @Data
  @Accessors(chain = true)
  private static class JobContainer {

    private UUID taskId;
    private String uniqueName;
    private IJob job;
  }
}
