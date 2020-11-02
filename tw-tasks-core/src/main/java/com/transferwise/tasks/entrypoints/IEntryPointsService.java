package com.transferwise.tasks.entrypoints;

import java.util.function.Supplier;

public interface IEntryPointsService {

  <T> T continueOrCreate(String group, String name, Supplier<T> supplier);
}
