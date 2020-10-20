package com.transferwise.tasks.utils;

import java.math.BigInteger;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class TwTasksUuidUtils {

  public static UUID toUuid(String st) {
    String hyphenlessUuid = StringUtils.remove(st, '-');
    BigInteger bigInteger = new BigInteger(hyphenlessUuid, 16);
    return new UUID(bigInteger.shiftRight(64).longValue(), bigInteger.longValue());
  }
}
