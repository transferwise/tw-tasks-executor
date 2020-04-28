package com.transferwise.tasks.domain;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.transferwise.tasks.utils.UuidUtils;
import java.io.IOException;
import java.util.UUID;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

@Data
@Accessors(chain = true)
@JsonSerialize(using = TaskVersionId.TaskVersionIdJsonSerializer.class)
@JsonDeserialize(using = TaskVersionId.TaskVersionIdJsonDeserializer.class)
public class TaskVersionId implements ITaskVersionId {

  private UUID id;
  private long version;

  public TaskVersionId() {
  }

  public TaskVersionId(UUID id, long version) {
    this.id = id;
    this.version = version;
  }

  public static class TaskVersionIdJsonSerializer extends StdSerializer<TaskVersionId> {

    private static final long serialVersionUID = 1L;

    public TaskVersionIdJsonSerializer() {
      super(TaskVersionId.class);
    }

    @Override
    public void serialize(TaskVersionId value, JsonGenerator gen, SerializerProvider provider) throws IOException {
      gen.writeString(value.getId().toString() + "-" + value.getVersion());
    }
  }

  public static class TaskVersionIdJsonDeserializer extends StdDeserializer<TaskVersionId> {

    private static final long serialVersionUID = 1L;

    public TaskVersionIdJsonDeserializer() {
      super(TaskVersionId.class);
    }

    @Override
    public TaskVersionId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String val = p.getValueAsString();
      int idx = StringUtils.lastIndexOf(val, '-');

      return new TaskVersionId().setId(UuidUtils.toUuid(StringUtils.substring(val, 0, idx)))
          .setVersion(Integer.parseInt(StringUtils.substring(val, idx + 1)));
    }
  }

  public String toString() {
    return id + "-" + version;
  }
}

