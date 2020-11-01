package com.transferwise.tasks;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * We need to seriously removing that code as it has not many use cases left in TW.
 */
@Retention(RetentionPolicy.SOURCE)
public @interface FeatureBloat {

  String value() default "";
}
