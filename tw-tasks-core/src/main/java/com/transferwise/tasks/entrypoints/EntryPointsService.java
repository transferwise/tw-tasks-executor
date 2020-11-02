package com.transferwise.tasks.entrypoints;

import com.transferwise.common.context.TwContext;
import com.transferwise.common.context.UnitOfWorkManager;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;

public class EntryPointsService implements IEntryPointsService {

  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  @Override
  public <T> T continueOrCreate(String group, String name, Supplier<T> supplier) {
    if (TwContext.current().isEntryPoint()) {
      return supplier.get();
    }

    return unitOfWorkManager.createEntryPoint(group, name).toContext().execute(supplier);
  }
}
