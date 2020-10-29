package com.transferwise.tasks.helpers.sql;

import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;

/**
 * A cache key, each weight is expected to be less than 8 (3 bits).
 */
@EqualsAndHashCode
public final class CacheKey {

  private static final int LEFT_SHIFT = 3;
  private static final int MAX_WEIGHT_EXCLUSIVE = 1 << LEFT_SHIFT;

  private final String name;
  private final int weightsSum;

  public CacheKey(String name, int weight1) {
    Preconditions.checkArgument(weight1 < MAX_WEIGHT_EXCLUSIVE);
    this.name = name;
    this.weightsSum = (1 << LEFT_SHIFT) + weight1;
  }

  public CacheKey(String name, int weight1, int weight2) {
    Preconditions.checkArgument(weight1 < MAX_WEIGHT_EXCLUSIVE && weight2 < MAX_WEIGHT_EXCLUSIVE);
    this.name = name;
    int weights = (1 << LEFT_SHIFT) + weight1;
    this.weightsSum = (weights << LEFT_SHIFT) + weight2;
  }
}
