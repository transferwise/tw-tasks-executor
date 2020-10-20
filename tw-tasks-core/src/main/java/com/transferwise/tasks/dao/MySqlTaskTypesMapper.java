package com.transferwise.tasks.dao;

import com.transferwise.common.baseutils.UuidUtils;
import java.util.UUID;

public class MySqlTaskTypesMapper implements TaskSqlMapper {

  @Override
  public UUID sqlTaskIdToUuid(Object taskId) {
    return UuidUtils.toUuid((byte[]) taskId);
  }

  @Override
  public Object uuidToSqlTaskId(UUID uuid) {
    return UuidUtils.toBytes(uuid);
  }
}
