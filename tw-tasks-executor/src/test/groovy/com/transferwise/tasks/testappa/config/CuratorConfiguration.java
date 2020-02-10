package com.transferwise.tasks.testappa.config;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.helpers.executors.ExecutorThreadFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CuratorConfiguration {

  @Bean(destroyMethod = "close")
  public CuratorFramework curatorFramework(@Autowired(required = false) TestContainersManager testContainersManager,
      TasksProperties tasksProperties) {
    CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectionTimeoutMs(5000).sessionTimeoutMs(300000).canBeReadOnly(false)
        .connectString(tasksProperties.getZookeeperConnectString()).retryPolicy(new RetryNTimes(5, 1000))
        .threadFactory(new ExecutorThreadFactory("curator")).build();

    curatorFramework.start();

    return curatorFramework;
  }
}
