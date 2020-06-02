package com.transferwise.tasks.impl.tokafka;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.Multimap;
import lombok.Data;
import lombok.experimental.Accessors;

public interface IToKafkaSenderService {

  /**
   * Sends a single Kafka message. Always prefer to use the #sendMessages method when dealing with multiple messages.
   *
   * <p>You can provide an Object in payload and it will be json-serialized or a plain string in payloadString.
   *
   * <p>Note that the implementation doesn't add a namespace to the topic, you need to do this yourself
   * (see for example KafkaHelper in Payout Service).
   *
   * <p>key - kafka message key, usually not needed sendAfterTime - if you want to delay the sending of message
   */
  void sendMessage(SendMessageRequest request);

  @Data
  @Accessors(chain = true)
  class SendMessageRequest {

    private String topic;
    private String key;
    private Object payload;
    private String payloadString;
    private Multimap<String, byte[]> headers;
    private ZonedDateTime sendAfterTime;
  }

  /**
   * Sends multiple messages. Input semantics is the same as in sendMessage method.
   */
  void sendMessages(SendMessagesRequest request);

  @Data
  @Accessors(chain = true)
  class SendMessagesRequest {

    private String topic;
    private ZonedDateTime sendAfterTime;

    private List<Message> messages = new ArrayList<>();

    public SendMessagesRequest add(Message message) {
      messages.add(message);
      return this;
    }

    @Data
    @Accessors(chain = true)
    public static class Message {

      private String key;
      private Object payload;
      private String payloadString;
      private Multimap<String, byte[]> headers;

    }
  }
}
