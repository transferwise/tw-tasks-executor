package com.transferwise.tasks.helpers.kafka.messagetotask;

import java.util.List;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface IKafkaMessageHandler<T> {

  List<Topic> getTopics();

  boolean handles(String topic);

  void handle(ConsumerRecord<String, T> record);

  @Data
  @Accessors(chain = true)
  class Topic {

    private String address;
    private Integer suggestedPartitionsCount;
  }
}
