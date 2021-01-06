package com.transferwise.tasks;

import com.vdurmont.semver4j.Semver;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class EnvironmentValidator implements IEnvironmentValidator {

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public void validate() {
    String previousVersion = tasksProperties.getEnvironment().getPreviousVersion();
    if (StringUtils.trimToNull(previousVersion) == null) {
      throw new IllegalStateException("Previous version has not been specified.");
    }

    new Semver(previousVersion);

    // Future checks. This version allows to upgrade from any earlier versions.
  }

}
