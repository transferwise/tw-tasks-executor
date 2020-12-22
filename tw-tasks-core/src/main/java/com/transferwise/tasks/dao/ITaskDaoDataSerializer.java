package com.transferwise.tasks.dao;

import com.transferwise.tasks.ITasksService.AddTaskRequest.CompressionRequest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.Data;
import lombok.experimental.Accessors;

@SuppressFBWarnings("EI_EXPOSE_REP")
public interface ITaskDaoDataSerializer {

  SerializeResult serialize(byte[] data, CompressionRequest compression);

  byte[] deserialize(int dataFormat, byte[] data);

  @Data
  @Accessors(chain = true)
  class SerializeResult {

    private int dataFormat;
    private byte[] data;
  }
}
