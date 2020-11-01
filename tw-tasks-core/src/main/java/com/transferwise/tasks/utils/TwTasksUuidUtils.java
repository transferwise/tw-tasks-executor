package com.transferwise.tasks.utils;

import java.math.BigInteger;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;

public final class TwTasksUuidUtils {

  private TwTasksUuidUtils() {
    throw new AssertionError();
  }

  public static UUID toUuid(String st) {
    String hyphenlessUuid = StringUtils.remove(st, '-');
    BigInteger bigInteger = new BigInteger(hyphenlessUuid, 16);
    return new UUID(bigInteger.shiftRight(64).longValue(), bigInteger.longValue());
  }
}
