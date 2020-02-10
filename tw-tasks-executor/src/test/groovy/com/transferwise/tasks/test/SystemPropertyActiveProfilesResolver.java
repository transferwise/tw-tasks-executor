package com.transferwise.tasks.test;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.test.context.ActiveProfilesResolver;
import org.springframework.test.context.support.DefaultActiveProfilesResolver;

public class SystemPropertyActiveProfilesResolver implements ActiveProfilesResolver {

  private final DefaultActiveProfilesResolver defaultActiveProfilesResolver = new DefaultActiveProfilesResolver();

  @Override
  public String[] resolve(Class<?> testClass) {
    String[] profiles = defaultActiveProfilesResolver.resolve(testClass);
    if (System.getProperties().containsKey("spring.profiles.include")) {
      final String include = System.getProperty("spring.profiles.include");
      profiles = ArrayUtils.addAll(profiles, include.split("\\s*,\\s*"));
    }
    return profiles;
  }
}
