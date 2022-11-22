package com.transferwise.tasks.dao;

import com.transferwise.common.baseutils.UuidUtils;
import java.util.UUID;

public class MySqlTaskTypesMapper implements ITaskSqlMapper {

  @Override
  public UUID sqlTaskIdToUuid(Object taskId) {
    if (taskId == null) {
      return null;
    }
    return UuidUtils.toUuid((byte[]) taskId);
  }

  @Override
  public Object uuidToSqlTaskId(UUID uuid) {
    if (uuid == null) {
      return null;
    }
    return UuidUtils.toBytes(uuid);
  }
}
