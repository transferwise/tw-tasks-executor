package com.transferwise.tasks.config;

import com.transferwise.tasks.TasksProperties.DbType;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class JdbcEnvironmentCondition implements Condition {
  private static final String DB_TYPE_KEY = "tw-tasks.core.db-type";

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return !DbType.MONGO.name().equalsIgnoreCase(context.getEnvironment().getProperty(DB_TYPE_KEY));
  }
}
