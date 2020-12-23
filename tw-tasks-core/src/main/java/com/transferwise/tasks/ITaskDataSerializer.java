package com.transferwise.tasks;

public interface ITaskDataSerializer {

  byte[] serialize(String data);

  byte[] serializeToJson(Object data);
}
