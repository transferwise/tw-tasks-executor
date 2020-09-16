package com.transferwise.tasks.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.transferwise.common.baseutils.UuidUtils;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.Test;

public class TwTasksUuidUtilsTest {

  @Test
  public void stringCanBeConvertedToUuid() {
    UUID uuid = UUID.fromString("8d876905-1f6f-4bdd-935f-73969b725955");

    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd-935f-73969b725955")).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd935f73969b725955")).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd935f73969B725955")).isEqualTo(uuid);
  }

  @Test
  public void differentArgumentsCanBeConvertedToUuid() {
    UUID uuid = UUID.fromString("8d876905-1f6f-4bdd-935f-73969b725955");

    assertThat(TwTasksUuidUtils.toUuid("8d876905-1f6f-4bdd-935f-73969b725955")).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid(uuid)).isEqualTo(uuid);
    assertThat(TwTasksUuidUtils.toUuid(UuidUtils.toBytes(uuid))).isEqualTo(uuid);

    assertThatThrownBy(() -> TwTasksUuidUtils.toUuid(new Object())).isInstanceOf(NotImplementedException.class);
  }
}
