package com.transferwise.tasks.impl.jobs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface CronJob {

  /**
   * A cron-like expression
   */
  String value();

  /**
   * a zone id accepted by java.util.TimeZone.getTimeZone(String), or an empty String to indicate the server's default time zone
   */
  String timezone() default "";

  boolean transactional() default false;

}