package com.transferwise.tasks.impl.tokafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Multimap;
import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.ITasksService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
public class ToKafkaSenderService implements IToKafkaSenderService {

  private static final int MEGABYTE_MULTIPLIER = 1_000_000;
  private static final double KILOBYTE_DENOM = 1024.0;

  private final ObjectMapper objectMapper;
  private final ITasksService taskService;
  private final int batchSizeMb;

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void sendMessage(SendMessageRequest request) {
    ToKafkaMessages messages = new ToKafkaMessages().setTopic(request.getTopic()).add(convert(request));

    taskService.addTask(new ITasksService.AddTaskRequest().setType(ToKafkaTaskType.VALUE).setSubType(messages.getTopic()).setData(messages)
        .setRunAfterTime(request.getSendAfterTime()));
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void sendMessages(SendMessagesRequest request) {
    List<ToKafkaMessages.Message> messageStream = request
        .getMessages()
        .stream()
        .map(this::convert)
        .collect(Collectors.toList());

    splitToBatches(messageStream, batchSizeMb * MEGABYTE_MULTIPLIER).forEach(it ->
        sendBatch(request, it));
  }

  protected ToKafkaMessages.Message convert(SendMessageRequest request) {
    return toKafkaMessage(request.getKey(), request.getPayloadString(), request.getPayload(), request.getHeaders());
  }

  protected ToKafkaMessages.Message convert(SendMessagesRequest.Message message) {
    return toKafkaMessage(message.getKey(), message.getPayloadString(), message.getPayload(), message.getHeaders());
  }

  protected List<List<ToKafkaMessages.Message>> splitToBatches(List<ToKafkaMessages.Message> messages, int batchSizeBytes) {
    if (messages.isEmpty()) {
      return Collections.emptyList();
    }

    List<List<ToKafkaMessages.Message>> batches = new ArrayList<>();
    int numBatches = 0;
    int batchSize = 0;
    List<ToKafkaMessages.Message> batch = new ArrayList<>();
    ToKafkaMessages.Message firstMessage = messages.get(0);
    int messageSize = firstMessage.getApproxSize();
    batchSize += messageSize;
    batch.add(firstMessage);

    for (ToKafkaMessages.Message message : messages.subList(1, messages.size())) {
      messageSize = message.getApproxSize();
      if (batchSize + messageSize > batchSizeBytes) {
        batches.add(batch);
        numBatches++;
        if (log.isDebugEnabled()) {
          log.debug("Big bunch of messages was received in one go. Splitting it to batches. {}-th batch size is {} messages, {} KiB", numBatches,
              batch.size(),
              batchSize / KILOBYTE_DENOM);
        }
        batch = new ArrayList<>();
        batchSize = 0;
      }
      batchSize += messageSize;
      batch.add(message);
    }
    if (numBatches != 0) {
      if (log.isDebugEnabled()) {
        log.debug("Big bunch of messages was received in one go. Splitting it to batches. {}-th batch size is {} messages, {} KiB", ++numBatches,
            batch.size(),
            batchSize / KILOBYTE_DENOM);
      }
    }
    batches.add(batch);

    return batches;
  }

  private void sendBatch(SendMessagesRequest request, List<ToKafkaMessages.Message> batch) {
    ToKafkaMessages messages = new ToKafkaMessages().setTopic(request.getTopic()).setMessages(batch);
    taskService.addTask(
        new ITasksService.AddTaskRequest()
            .setType(ToKafkaTaskType.VALUE)
            .setSubType(messages.getTopic())
            .setData(messages)
            .setRunAfterTime(request.getSendAfterTime()));
  }

  private ToKafkaMessages.Message toKafkaMessage(String key, String payloadString, Object payload, Multimap<String, byte[]> headers) {
    ToKafkaMessages.Message toKafkaMessage = new ToKafkaMessages.Message().setKey(key);
    if (payloadString != null) {
      toKafkaMessage.setMessage(payloadString);
    } else {
      toKafkaMessage.setMessage(ExceptionUtils.doUnchecked(() -> objectMapper.writeValueAsString(payload)));
    }

    toKafkaMessage.setHeaders(headers);

    return toKafkaMessage;
  }
}
