package com.transferwise.tasks.utils;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;

class UuidUtilTest {

  @Test
  void convertingFromUuidAndBackToBytesEndWithTheSameResult() {
    UUID expected = UUID.randomUUID();

    byte[] bytes = UuidUtils.toBytes(expected);
    UUID result = UuidUtils.toUuid(bytes);

    assertEquals(expected, result);
  }

  @Test
  void convertingFromBytesAndBackToUuidEndWithTheSameResult() {
    byte[] expected = RandomUtils.nextBytes(16);

    UUID uuid = UuidUtils.toUuid(expected);
    byte[] result = UuidUtils.toBytes(uuid);

    assertArrayEquals(expected, result);
  }
}
