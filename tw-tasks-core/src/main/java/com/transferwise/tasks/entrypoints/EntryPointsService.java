package com.transferwise.tasks.entrypoints;

import com.transferwise.common.context.TwContext;
import com.transferwise.common.context.UnitOfWorkManager;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;

public class EntryPointsService implements IEntryPointsService {

  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  @Autowired
  private MdcService mdcService;

  @Override
  public <T> T continueOrCreate(String group, String name, Supplier<T> supplier) {
    if (TwContext.current().isEntryPoint()) {
      return mdcService.with(() -> supplier.get());
    }

    return createEntrypoint(group, name, supplier);
  }

  @Override
  public <T> T createEntrypoint(String group, String name, Supplier<T> supplier) {
    return unitOfWorkManager.createEntryPoint(group, name).toContext().execute(mdcService.with(() -> supplier));
  }
}
