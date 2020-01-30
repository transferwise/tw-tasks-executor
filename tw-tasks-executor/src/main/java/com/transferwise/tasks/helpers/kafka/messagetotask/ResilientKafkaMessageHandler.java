package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.utils.TriConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A handler similar to {@link KafkaMessageToTaskConverter} but
 * resiliently handling corrupted (failed to deserialize) messages from kafka topic.
 * The decision on how to handle the corrupted message is delegated to {@link CorruptedMessageRecoveryStrategy}.
 */
@Slf4j
class ResilientKafkaMessageHandler<T> implements IKafkaMessageHandler<String> {

    private final ObjectMapper objectMapper;
    private final ITasksService tasksService;
    private final CorruptedMessageRecoveryStrategy corruptedMessageRecoveryStrategy;
    private final List<Topic> topics;
    private final Class<T> dataObjClass;
    private final TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer;

    ResilientKafkaMessageHandler(ObjectMapper objectMapper,
                                 ITasksService tasksService,
                                 CorruptedMessageRecoveryStrategy corruptedMessageRecoveryStrategy,
                                 Class<T> dataObjClass,
                                 TriConsumer<T, ConsumerRecord<String, String>, ITasksService.AddTaskRequest> consumer,
                                 Topic... topics) {
        this.objectMapper = objectMapper;
        this.tasksService = tasksService;
        this.corruptedMessageRecoveryStrategy = corruptedMessageRecoveryStrategy;
        this.dataObjClass = dataObjClass;
        this.consumer = consumer;
        this.topics = Arrays.asList(topics);
    }

    @Override
    public List<Topic> getTopics() {
        return topics;
    }

    @Override
    public boolean handles(String checkedTopic) {
        for (Topic topic : topics) {
            if (checkedTopic.endsWith(topic.getAddress())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void handle(ConsumerRecord<String, String> record) {
        T dataObj;
        try {
            dataObj = objectMapper.readValue(record.value(), this.dataObjClass);
        } catch (IOException x) {
            ExceptionUtils.doUnchecked(() -> corruptedMessageRecoveryStrategy.recover(dataObjClass, record, x));
            return;
        }

        ITasksService.AddTaskRequest addTaskRequest = new ITasksService.AddTaskRequest();
        if (consumer.accept(dataObj, record, addTaskRequest)) {
            tasksService.addTask(addTaskRequest);
        }
    }
}
