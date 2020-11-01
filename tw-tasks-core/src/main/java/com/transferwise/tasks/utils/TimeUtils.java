package com.transferwise.tasks.utils;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public final class TimeUtils {

  private TimeUtils() {
    throw new AssertionError();
  }

  public static ZonedDateTime toZonedDateTime(Timestamp timestamp) {
    if (timestamp == null) {
      return null;
    }
    return ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneId.systemDefault());
  }
}
