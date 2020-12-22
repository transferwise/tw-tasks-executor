package com.transferwise.tasks;

public interface ITaskDataSerializer {

  byte[] serialize(String data);

  byte[] serializeAsJson(Object data);
}
