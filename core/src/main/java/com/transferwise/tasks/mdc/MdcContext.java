package com.transferwise.tasks.mdc;

import com.transferwise.common.baseutils.ExceptionUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.MDC;

public class MdcContext {

  private static final ThreadLocal<Set<String>> keysSet = new ThreadLocal<>();

  private MdcContext() {
    throw new UnsupportedOperationException();
  }

  private static Set<String> getKeysSet() {
    Set<String> localKeysSet = keysSet.get();

    if (localKeysSet == null) {
      keysSet.set(localKeysSet = new HashSet<>());
    }

    return localKeysSet;
  }

  public static void put(String key, Object value) {
    if (value == null) {
      MDC.remove(key);
      getKeysSet().remove(key);
    } else {
      MDC.put(key, String.valueOf(value));
      getKeysSet().add(key);
    }
  }

  public static void clear() {
    MDC.clear();
  }

  public static void with(Runnable runnable) {
    with(() -> {
      runnable.run();
      return null;
    });
  }

  public static <T> T with(Callable<T> callable) {
    Map<String, String> previousMap = MDC.getCopyOfContextMap();
    try {
      return ExceptionUtils.doUnchecked(callable);
    } finally {
      for (String key : getKeysSet()) {
        if (previousMap != null && previousMap.containsKey(key)) {
          MDC.put(key, previousMap.get(key));
        } else {
          MDC.remove(key);
        }
      }
      keysSet.remove();
    }
  }

}
