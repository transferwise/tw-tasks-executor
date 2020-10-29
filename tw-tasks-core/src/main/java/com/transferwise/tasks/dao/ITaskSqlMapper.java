package com.transferwise.tasks.dao;

import java.util.UUID;

public interface ITaskSqlMapper {

  UUID sqlTaskIdToUuid(Object object);

  Object uuidToSqlTaskId(UUID uuid);
}
