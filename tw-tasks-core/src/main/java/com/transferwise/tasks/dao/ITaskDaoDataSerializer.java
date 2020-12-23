package com.transferwise.tasks.dao;

import com.transferwise.tasks.ITasksService.AddTaskRequest.CompressionRequest;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.experimental.Accessors;

@SuppressFBWarnings("EI_EXPOSE_REP")
public interface ITaskDaoDataSerializer {

  SerializedData serialize(@Nonnull byte[] data, CompressionRequest compression);

  byte[] deserialize(SerializedData serializedData);

  @Data
  @Accessors(chain = true)
  class SerializedData {

    private int dataFormat;
    private byte[] data;
  }
}
