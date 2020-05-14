package com.transferwise.tasks.utils;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.net.InetAddress;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
public class ClientIdUtils {

  public static String clientIdFromHostname() {
    String hostName = ExceptionUtils.doUnchecked(() -> InetAddress.getLocalHost().getHostName());
    String clientId = hostName.replaceAll("[^\\p{Alpha}\\p{Digit}]", "");
    if (StringUtils.isEmpty(clientId)) {
      throw new IllegalStateException("Could not get clientId from hostname '" + hostName + "'.");
    }
    return clientId;
  }
}
