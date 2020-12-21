package com.transferwise.tasks;

import io.micrometer.core.instrument.Tag;
import java.util.concurrent.ThreadLocalRandom;

public enum CompressionAlgorithm {
  NONE,
  /**
   * Fastest.
   */
  LZ4,
  /**
   * Best compression rate.
   */
  GZIP,
  // For complex tests
  RANDOM;

  private Tag micrometerTag = Tag.of("algorithm", name().toLowerCase());

  public Tag getMicrometerTag() {
    return micrometerTag;
  }

  public static CompressionAlgorithm getRandom() {
    switch (ThreadLocalRandom.current().nextInt(3)) {
      case 0:
        return NONE;
      case 1:
        return LZ4;
      default:
        return GZIP;
    }
  }
}
