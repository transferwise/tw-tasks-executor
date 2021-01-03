package com.transferwise.tasks;

import com.google.common.primitives.Ints;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class MigrationHandler implements IMigrationHandler {

  @Autowired
  private TasksProperties tasksProperties;

  @Override
  public void migrate() {
    String previousVersion = tasksProperties.getMigration().getPreviousVersion();
    if (StringUtils.trimToNull(previousVersion) == null) {
      throw new IllegalStateException("Previous version has not been specified.");
    }

    SemVer.parse(previousVersion);

    // Future checks. This version allows to upgrade from any earlier versions.
  }

  @AllArgsConstructor
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private static class SemVer {

    private int major;
    private int minor;
    private int patch;

    private static SemVer parse(String version) {
      String[] parts = version.split("\\.");
      if (parts.length == 3) {
        Integer major = Ints.tryParse(parts[0]);
        Integer minor = Ints.tryParse(parts[1]);
        Integer patch = Ints.tryParse(parts[2]);
        if (major != null && minor != null && patch != null) {
          return new SemVer(major, minor, patch);
        }
      }
      throw new IllegalArgumentException("Invalid version provided: '" + version + "'.");
    }
  }
}
