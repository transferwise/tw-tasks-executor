package com.transferwise.tasks;

import com.vdurmont.semver4j.Semver;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class BaseEnvironmentValidator implements IEnvironmentValidator {

  @Autowired
  protected TasksProperties tasksProperties;

  @Override
  public void validate() {
    var previousVersion = tasksProperties.getEnvironment().getPreviousVersion();
    if (StringUtils.trimToNull(previousVersion) == null) {
      throw new IllegalStateException("Previous version has not been specified.");
    }

    Semver previousSemver = new Semver(previousVersion);
    Semver minimumRequiredPreviousSemver = new Semver("1.21.1");

    if (previousSemver.compareTo(minimumRequiredPreviousSemver) < 0) {
      throw new IllegalStateException("This version requires at least '" + minimumRequiredPreviousSemver + "' to be deployed first.");
    }
  }
}
