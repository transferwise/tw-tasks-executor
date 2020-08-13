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

    /**
     * Topics in the same shard will be listened by only one Kafka Consumer.
     * 
     * <p>So the performance of one topic will be heavily dependent how other topics are performing or how many messages are spammed into those.
     * 
     * <p>Putting every topic into a separate shard has a downside of having many kafka consumers, which can put more load on the Kafka server.
     */
    private int shard = 0;
  }
}
