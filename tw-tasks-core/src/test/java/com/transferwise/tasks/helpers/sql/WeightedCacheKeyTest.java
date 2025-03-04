package com.transferwise.tasks.helpers.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class WeightedCacheKeyTest {

  @Test
  void testSingleWeightConstructorWithInvalidWeight() {
    assertThrows(IllegalArgumentException.class, () -> new WeightedCacheKey("testKey", 32));
  }

  @Test
  void testDoubleWeightConstructorWithInvalidWeight() {
    assertThrows(IllegalArgumentException.class, () -> new WeightedCacheKey("testKey", 10, 32));
  }

  @Test
  void testEqualsAndHashCode() {
    WeightedCacheKey cacheKey1 = new WeightedCacheKey("testKey", 10);
    WeightedCacheKey cacheKey2 = new WeightedCacheKey("testKey", 10);
    assertEquals(cacheKey1, cacheKey2);
    assertEquals(cacheKey1.hashCode(), cacheKey2.hashCode());

    WeightedCacheKey cacheKey3 = new WeightedCacheKey("testKey", 8);
    assertNotEquals(cacheKey1, cacheKey3);
    assertNotEquals(cacheKey1.hashCode(), cacheKey3.hashCode());

    WeightedCacheKey cacheKey4 = new WeightedCacheKey("no", 10);
    assertNotEquals(cacheKey1, cacheKey4);
    assertNotEquals(cacheKey1.hashCode(), cacheKey4.hashCode());
  }
}