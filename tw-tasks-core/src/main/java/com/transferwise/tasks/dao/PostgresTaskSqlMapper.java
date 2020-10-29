package com.transferwise.tasks.dao;

import java.util.UUID;

public class PostgresTaskSqlMapper implements ITaskSqlMapper {

  @Override
  public UUID sqlTaskIdToUuid(Object object) {
    return (UUID) object;
  }

  @Override
  public Object uuidToSqlTaskId(UUID uuid) {
    return uuid;
  }
}
