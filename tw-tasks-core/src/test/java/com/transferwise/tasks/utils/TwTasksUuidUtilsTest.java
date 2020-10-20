package com.transferwise.tasks.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TwTasksUuidUtilsTest {

  @Test
  public void stringCanBeConvertedToUuid() {
    UUID uuid = UUID.fromString("8d876905-1f6f-4bdd-935f-73969b725955");

    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd-935f-73969b725955")).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd935f73969b725955")).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd935f73969B725955")).isEqualTo(uuid);
  }
}
