package com.transferwise.tasks.dao;

/**
 * Resolves task table names according to DB naming convention.
 */
public interface TwTaskTables {

  String getTaskTableIdentifier();

  String getUniqueTaskKeyTableIdentifier();
}
