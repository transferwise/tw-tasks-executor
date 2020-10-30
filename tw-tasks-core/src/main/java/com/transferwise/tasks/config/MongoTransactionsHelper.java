package com.transferwise.tasks.config;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.common.baseutils.transactionsmanagement.ITransactionsHelper;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import java.util.concurrent.Callable;

public class MongoTransactionsHelper implements ITransactionsHelper {

  private final MongoTransactionManager transactionManager;

  public MongoTransactionsHelper(MongoTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
  }

  @Override
  public boolean isRollbackOnly() {
    return transactionManager.getTransaction(null).isRollbackOnly();
  }

  @Override
  public void markAsRollbackOnly() {
    transactionManager.getTransaction(null).setRollbackOnly();
  }

  @Override
  public IBuilder withTransaction() {
    return new Builder(transactionManager);
  }

  private static class Builder implements IBuilder {

    private Propagation propagation;
    private final MongoTransactionManager transactionManager;
    private boolean readOnly;
    private String name;
    private Isolation isolation;
    private Integer timeout;

    private Builder(MongoTransactionManager transactionManager) {
      this.transactionManager = transactionManager;
    }

    @Override
    public IBuilder withPropagation(Propagation propagation) {
      this.propagation = propagation;
      return this;
    }

    @Override
    public IBuilder asNew() {
      this.propagation = Propagation.REQUIRES_NEW;
      return this;
    }

    @Override
    public IBuilder asSuspended() {
      this.propagation = Propagation.NOT_SUPPORTED;
      return this;
    }

    @Override
    public IBuilder asReadOnly() {
      return asReadOnly(true);
    }

    @Override
    public IBuilder asReadOnly(boolean readOnly) {
      this.readOnly = readOnly;
      return this;
    }

    @Override
    public IBuilder withName(String name) {
      this.name = name;
      return this;
    }

    @Override
    public IBuilder withIsolation(Isolation isolation) {
      this.isolation = isolation;
      return this;
    }

    @Override
    public IBuilder withTimeout(Integer timeout) {
      this.timeout = timeout;
      return this;
    }

    @Override
    public <T> T call(Callable<T> callable) {
      return ExceptionUtils.doUnchecked(() -> {
        DefaultTransactionDefinition def = new DefaultTransactionDefinition();
        if (propagation != null) {
          def.setPropagationBehavior(propagation.value());
        }
        def.setReadOnly(readOnly);
        def.setName(name);
        if (isolation != null) {
          def.setIsolationLevel(isolation.value());
        }
        if (timeout != null) {
          def.setTimeout(timeout);
        }

        TransactionStatus status = transactionManager.getTransaction(def);
        T result;
        try {
          result = callable.call();
        } catch (Throwable t) {
          transactionManager.rollback(status);
          throw t;
        }
        transactionManager.commit(status);
        return result;
      });
    }
  }
}
