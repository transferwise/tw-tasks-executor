package com.transferwise.tasks.impl.tokafka;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class ToKafkaMessages {

  private String topic;

  private List<Message> messages = new ArrayList<>();

  public ToKafkaMessages add(Message message) {
    messages.add(message);
    return this;
  }

  @Data
  @Accessors(chain = true)
  public static class Message {

    private static final int JSON_OVERHEAD = "{key: '', message: ''}".getBytes(StandardCharsets.UTF_8).length + 4;

    private String key;
    private String message;

    @JsonIgnore
    public int getApproxSize() {
      return getMessage().getBytes(StandardCharsets.UTF_8).length + String.valueOf(getKey()).getBytes(StandardCharsets.UTF_8).length + JSON_OVERHEAD;
    }
  }
}
