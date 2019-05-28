package com.transferwise.tasks.demoapp.config;

import com.transferwise.tasks.helpers.executors.ExecutorThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class CuratorConfiguration {
    @Value("${zookeeper.connect-string}")
    private String connectString;

    @Value("${curator.session-timeout-ms:60000}")
    private int sessionTimeoutMs;

    @Value("${curator.connection-timeout-ms:15000}")
    private int connectionTimeoutMs;

    @Value("${curator.retry-timeout-ms:5000}")
    private int retryTimeoutMs;

    @Bean(destroyMethod = "close")
    public CuratorFramework curatorFramework() {
        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder()
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .canBeReadOnly(false)
            .connectString(connectString)
            .retryPolicy(new RetryForever(retryTimeoutMs))
            .threadFactory(new ExecutorThreadFactory("twtasks-curator"))
            .build();

        log.info("Started curator framework with connection timeout of " + connectionTimeoutMs + " ms, session timeout of " + sessionTimeoutMs + " ms, " +
            "retry timeout of " + retryTimeoutMs + " and connect string of '" + connectString + "'.");

        curatorFramework.start();
        return curatorFramework;
    }
}
