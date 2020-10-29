package com.transferwise.tasks.helpers.sql;

/**
 * Provides useful methods for working with plain sql queries.
 */
public final class SqlHelper {

  public static final String PARAMETERS_PLACEHOLDER = "??";

  /**
   * Replaces {@code ??} with a number of {@code ?} equal to {@code count}.
   */
  public static String expandParametersList(String sql, int count) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append("?");
      if (i + 1 < count) {
        sb.append(",");
      }
    }
    return sql.replace(PARAMETERS_PLACEHOLDER, sb.toString());
  }
}
