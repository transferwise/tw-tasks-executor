package com.transferwise.tasks.dao;

import static java.lang.annotation.ElementType.METHOD;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Query does not need to provide real time or consecutively consistent results.
 */
@Documented
@Retention(RetentionPolicy.SOURCE)
@Target(value = {METHOD})
public @interface MonitoringQuery {

  // Alias for comment
  String value() default "";

  String[] comment() default {};
}
