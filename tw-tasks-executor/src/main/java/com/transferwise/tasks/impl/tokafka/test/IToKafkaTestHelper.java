package com.transferwise.tasks.impl.tokafka.test;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;

public interface IToKafkaTestHelper {
    <T> List<T> getSentKafkaMessages(String topic, Class<T> clazz);

    ToKafkaTestHelper.SendKafkaEventHandler trackKafkaMessagesEvents();

    void sendDirectKafkaMessage(String topic, Object data);

    void sendDirectKafkaMessage(String topic, Long timestamp, String key, Object data);

    void sendDirectKafkaMessage(ProducerRecord<String, String> producerRecord);

    void cleanFinishedTasks(String topic);
}
