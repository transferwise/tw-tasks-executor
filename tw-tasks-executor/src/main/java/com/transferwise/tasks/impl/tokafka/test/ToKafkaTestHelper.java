package com.transferwise.tasks.impl.tokafka.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.impl.tokafka.ToKafkaMessages;
import com.transferwise.tasks.impl.tokafka.ToKafkaTaskType;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.test.TaskTrackerHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class ToKafkaTestHelper implements IToKafkaTestHelper {
    @Autowired
    private ITestTasksService testTasksService;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    @Override
    public <T> List<T> getSentKafkaMessages(String topic, Class<T> clazz) {
        return ExceptionUtils.doUnchecked(() -> {
            List<T> result = new ArrayList<>();
            List<Task> finishedTasks = testTasksService.getFinishedTasks(ToKafkaTaskType.VALUE, topic);
            if (finishedTasks.isEmpty()) {
                return result;
            }

            for (Task task : finishedTasks) {
                if (!TaskStatus.DONE.name().equals(task.getStatus())) {
                    continue;
                }
                ToKafkaMessages toKafkaMessages = objectMapper.readValue(task.getData(), ToKafkaMessages.class);
                for (ToKafkaMessages.Message toKafkaMessage : toKafkaMessages.getMessages()) {
                    result.add(objectMapper.readValue(toKafkaMessage.getMessage(), clazz));
                }
            }
            return result;
        });
    }

    @RequiredArgsConstructor
    public static class SendKafkaEventHandler implements AutoCloseable {
        private final TaskTrackerHandler interceptAddTasks;
        private final ObjectMapper objectMapper;
        private final ITestTasksService testTasksService;

        public Stream<ToKafkaMessages.Message> getSentKafkaMessages() {
            return interceptAddTasks
                .getRequests()
                .stream()
                .map(it -> it.getData() != null ? (ToKafkaMessages) it.getData() : ExceptionUtils.doUnchecked(() ->
                    objectMapper.readValue(it.getDataString(), ToKafkaMessages.class)))
                .flatMap(it -> it.getMessages().stream());
        }

        public Stream<ToKafkaMessages.Message> getSentKafkaMessages(String topic) {
            return interceptAddTasks
                .getRequests()
                .stream()
                .filter(it -> topic.equals(it.getSubType()))
                .map(it -> it.getData() != null ? (ToKafkaMessages) it.getData() : ExceptionUtils.doUnchecked(() ->
                    objectMapper.readValue(it.getDataString(), ToKafkaMessages.class)))
                .flatMap(it -> it.getMessages().stream());
        }

        public <T> Stream<T> getSentKafkaMessages(String topic, Class<T> messageClass) {
            return getSentKafkaMessages(topic).map(it -> ExceptionUtils.doUnchecked(() -> objectMapper.readValue(it.getMessage(), messageClass)));
        }

        public void close() {
            testTasksService.stopTracking(interceptAddTasks);
        }
    }

    @Override
    public SendKafkaEventHandler trackKafkaMessagesEvents() {
        return new SendKafkaEventHandler(testTasksService.startTrackingAddTasks(it -> ToKafkaTaskType.VALUE.equals(it.getType())), objectMapper, testTasksService);
    }

    @Override
    public void sendDirectKafkaMessage(String topic, Object data) {
        sendDirectKafkaMessage(topic, null, null, data);
    }

    @Override
    public void sendDirectKafkaMessage(String topic, Long timestamp, String key, Object data) {
        String dataAsString = data instanceof String ? (String) data : ExceptionUtils.doUnchecked(() -> objectMapper.writeValueAsString(data));
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, null, timestamp, key, dataAsString);
        sendDirectKafkaMessage(producerRecord);
    }

    @Override
    public void sendDirectKafkaMessage(ProducerRecord<String, String> producerRecord) {
        kafkaTemplate.send(producerRecord)
            .addCallback(
                result -> log.debug("Sent and acked Kafka message to topic '{}'.", producerRecord.topic()),
                exception -> log.error("Sending message to Kafka topic '{}'.", producerRecord.topic(), exception));
    }

    @Override
    public void cleanFinishedTasks(String topic) {
        testTasksService.cleanFinishedTasks(ToKafkaTaskType.VALUE, topic);
    }
}
