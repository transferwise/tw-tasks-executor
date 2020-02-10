package com.transferwise.tasks.test;

import org.springframework.context.ApplicationContext;

public class TestApplicationContextHolder {

  private static ApplicationContext applicationContext;

  public static ApplicationContext getApplicationContext() {
    return applicationContext;
  }

  public static void setApplicationContext(ApplicationContext applicationContext) {
    TestApplicationContextHolder.applicationContext = applicationContext;
  }
}

