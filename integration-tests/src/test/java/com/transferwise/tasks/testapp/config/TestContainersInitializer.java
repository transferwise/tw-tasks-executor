package com.transferwise.tasks.testapp.config;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.SocketUtils;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.SocatContainer;

@Slf4j
public class TestContainersInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {

  private static DockerComposeContainer container;

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    log.info("Active profiles: " + StringUtils.join(applicationContext.getEnvironment().getActiveProfiles(), ", "));

    if ("false".equals(applicationContext.getEnvironment().getProperty("testcontainers.enabled"))) {
      return;
    }

    ExceptionUtils.doUnchecked(() -> {
      if (container == null) {
        int kafka1Port = SocketUtils.findAvailableTcpPort();

        Path tempDirectory = Files.createTempDirectory(Paths.get("/tmp"), "tw-tasks-tests");

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        org.springframework.core.io.Resource[] resources = resolver.getResources("testcontainers/**/*");
        for (org.springframework.core.io.Resource resource : resources) {
          try (OutputStream os = new BufferedOutputStream(new FileOutputStream(new File(tempDirectory.toFile(), resource.getFilename())))) {
            IOUtils.copy(new BufferedInputStream(resource.getInputStream()), os);
          }
        }
        container = new DockerComposeContainer(new File(tempDirectory.toFile(), "compose-test.yml"))
            .withExposedService("zk-service1", 2181)
            .withExposedService("kafka1", 9093)
            .withExposedService("mysql1", 3306)
            .withExposedService("postgres1", 5432)
            .withTailChildContainers(true)
            .withPull(false)
            .withEnv("KAFKA1_EXTERNAL_PORT", "" + kafka1Port);

        ExceptionUtils.doUnchecked(() -> {
          Field f = DockerComposeContainer.class.getDeclaredField("ambassadorContainer");
          ReflectionUtils.makeAccessible(f);
          SocatContainer socatContainer = (SocatContainer) f.get(container);
          socatContainer.getPortBindings().add(kafka1Port + ":2001");
        });

        container.start();
      }

      Integer mysql1Port = container.getServicePort("mysql1", 3306);
      Integer postgres1Port = container.getServicePort("postgres1", 5432);
      Integer zkService1Port = container.getServicePort("zk-service1", 2181);
      Integer kafka1Port = container.getServicePort("kafka1", 9093);

      Map<String, Object> props = new HashMap<>();
      props.put("testenv.mysql.port", mysql1Port);
      props.put("testenv.postgres.port", postgres1Port);
      props.put("testenv.zookeeper.port", zkService1Port);
      props.put("testenv.kafka.port", kafka1Port);
      MapPropertySource propertySource = new MapPropertySource("testContainers", props);
      applicationContext.getEnvironment().getPropertySources().addFirst(propertySource);

      log.info("Started testcontainers docker-compose.");
      log.info("MySQL running on port " + mysql1Port);
      log.info("Postgres running on port " + postgres1Port);
      log.info("Zookeeper running on port " + zkService1Port);
      log.info("Kafka running on port " + kafka1Port);
      applicationContext.getBeanFactory().registerSingleton("dockerComposeContainer", container);
    });
  }
}
