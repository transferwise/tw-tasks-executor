package com.transferwise.tasks.utils;

import com.transferwise.common.baseutils.UuidUtils;
import java.math.BigInteger;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class TwTasksUuidUtils {

  public static UUID toUuid(String st) {
    String hyphenlessUuid = StringUtils.remove(st, '-');
    BigInteger bigInteger = new BigInteger(hyphenlessUuid, 16);
    return new UUID(bigInteger.shiftRight(64).longValue(), bigInteger.longValue());
  }

  public static UUID toUuid(Object arg) {
    if (arg == null) {
      return null;
    } else if (arg instanceof UUID) {
      return (UUID) arg;
    } else if (arg instanceof byte[]) {
      return UuidUtils.toUuid((byte[]) arg);
    }

    throw new NotImplementedException("" + arg.getClass().getName() + " is not supported.");
  }

}
