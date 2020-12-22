package com.transferwise.tasks.dao;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Data;
import lombok.experimental.Accessors;

@SuppressFBWarnings("EI_EXPOSE_REP")
public interface ITaskDataSerializer {

  SerializeResult serialize(byte[] data);

  byte[] deserialize(int dataFormat, byte[] data);

  @Data
  @Accessors(chain = true)
  class SerializeResult {

    private int dataFormat;
    private byte[] data;
  }
}
