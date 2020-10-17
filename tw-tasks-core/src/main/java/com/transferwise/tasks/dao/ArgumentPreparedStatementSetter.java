package com.transferwise.tasks.dao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;

public class ArgumentPreparedStatementSetter implements PreparedStatementSetter {

  private final DbConvention dbConvention;
  private final Object[] args;

  public ArgumentPreparedStatementSetter(DbConvention dbConvention, List<Object> args) {
    this.dbConvention = dbConvention;
    this.args = args.toArray();
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ArgumentPreparedStatementSetter(DbConvention dbConvention, Object[] args) {
    this.dbConvention = dbConvention;
    this.args = args;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void setValues(PreparedStatement ps) throws SQLException {
    int idx = 0;
    for (Object arg : args) {
      if (arg instanceof Object[]) {
        Object[] subArgs = (Object[]) arg;
        for (Object subArg : subArgs) {
          doSetValue(ps, ++idx, subArg);
        }
      } else if (arg instanceof List) {
        List<Object> subArgs = (List<Object>) arg;
        for (Object subArg : subArgs) {
          doSetValue(ps, ++idx, subArg);
        }
      } else {
        doSetValue(ps, ++idx, arg);
      }
    }
  }

  protected void doSetValue(PreparedStatement ps, int parameterPosition, Object argValue) throws SQLException {
    if (argValue instanceof SqlParameterValue) {
      SqlParameterValue paramValue = (SqlParameterValue) argValue;
      StatementCreatorUtils.setParameterValue(ps, parameterPosition, paramValue, paramValue.getValue());
    } else {
      if (argValue instanceof UUID) {
        argValue = dbConvention.uuidAsPsArgument((UUID) argValue);
      } else if (argValue instanceof Instant) {
        argValue = Timestamp.from((Instant) argValue);
      } else if (argValue instanceof TemporalAccessor) {
        argValue = Timestamp.from(Instant.from((TemporalAccessor) argValue));
      } else if (argValue instanceof Enum<?>) {
        argValue = ((Enum<?>) argValue).name();
      }
      StatementCreatorUtils.setParameterValue(ps, parameterPosition, SqlTypeValue.TYPE_UNKNOWN, argValue);
    }
  }
}
