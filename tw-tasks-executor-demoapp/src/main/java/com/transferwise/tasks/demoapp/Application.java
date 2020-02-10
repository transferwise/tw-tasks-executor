package com.transferwise.tasks.demoapp;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@SpringBootApplication
@EnableTransactionManagement
@EnableWebMvc
@Slf4j
public class Application {

  public static void main(String[] args) {
    try {
      catchUncaughtExceptions();
      SpringApplication springApplication = new SpringApplication(Application.class);
      springApplication.addListeners(new ApplicationPidFileWriter());
      springApplication.run(args);
    } catch (Throwable t) {
      if (!t.getClass().getSimpleName().equals("SilentExitException")) {
        log.error(t.getMessage(), t);
      }
      throw t;
    }
  }

  private static void catchUncaughtExceptions() {
    Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
      log.error("Uncaught exception occured in '" + t.getId() + "'. Contact developers immediately.", e);
      e.printStackTrace(); // In case logging system is messed up.
    });
  }
}
