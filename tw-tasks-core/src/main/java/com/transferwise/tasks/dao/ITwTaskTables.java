package com.transferwise.tasks.dao;

/**
 * Resolves task table names according to DB naming convention.
 */
public interface ITwTaskTables {

  String getTaskTableIdentifier();

  String getUniqueTaskKeyTableIdentifier();

  String getTaskDataTableIdentifier();
}
