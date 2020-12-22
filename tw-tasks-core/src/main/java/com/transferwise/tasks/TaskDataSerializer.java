package com.transferwise.tasks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import java.nio.charset.StandardCharsets;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskDataSerializer implements ITaskDataSerializer {

  @Autowired
  private ObjectMapper objectMapper;

  @Override
  public byte[] serialize(String data) {
    return data == null ? null : data.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public byte[] serializeAsJson(Object data) {
    return data == null ? null : ExceptionUtils.doUnchecked(() -> objectMapper.writeValueAsBytes(data));
  }
}
