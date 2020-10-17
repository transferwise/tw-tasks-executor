package com.transferwise.tasks.dao;

import java.util.UUID;

/**
 * Naming/types convention that could differ per DB.
 */
public interface DbConvention {

  String getTaskTableIdentifier();

  String getUniqueTaskKeyTableIdentifier();

  Object uuidAsPsArgument(UUID uuid);
}
