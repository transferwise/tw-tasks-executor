package com.transferwise.tasks.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

@Slf4j
public class BaseExtension implements BeforeTestExecutionCallback, AfterTestExecutionCallback {

  private static final String START_TIME = "start time";

  @Override
  public void afterTestExecution(ExtensionContext context) {
    long startTime = getStore(context).remove(START_TIME, long.class);
    long duration = System.currentTimeMillis() - startTime;

    log.info("Test '{}' ended in {} ms.", getTestName(context), duration);
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    log.info("Starting test '{}'.", getTestName(context));
    getStore(context).put(START_TIME, System.currentTimeMillis());
  }

  private String getTestName(ExtensionContext context) {
    return context.getRequiredTestMethod() + ": " + context.getDisplayName();
  }

  private Store getStore(ExtensionContext context) {
    return context.getStore(Namespace.create(getClass(), context.getRequiredTestMethod()));
  }
}
