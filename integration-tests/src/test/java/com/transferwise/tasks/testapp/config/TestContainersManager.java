package com.transferwise.tasks.testapp.config;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.DockerComposeContainer;

@Slf4j
public class TestContainersManager {

  @Autowired
  DockerComposeContainer dockerComposeContainer;

  private int registrationCount = 0;

  @PostConstruct
  public void init() {
    registrationCount++;
  }

  @PreDestroy
  public void destroy() {
    registrationCount--;
    if (registrationCount == 0) {
      log.info("Stopping docker compose container.");
      dockerComposeContainer.stop();
    }
  }
}
