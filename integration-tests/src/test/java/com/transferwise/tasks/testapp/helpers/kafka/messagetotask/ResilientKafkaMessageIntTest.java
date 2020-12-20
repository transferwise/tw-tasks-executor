package com.transferwise.tasks.testapp.helpers.kafka.messagetotask;

import static com.transferwise.tasks.helpers.kafka.messagetotask.CreateTaskForCorruptedMessageRecoveryStrategy.DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.helpers.kafka.messagetotask.CreateTaskForCorruptedMessageRecoveryStrategy.CorruptedKafkaMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

class ResilientKafkaMessageIntTest extends BaseIntTest {

  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  private ObjectMapper objectMapper;

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @AfterEach
  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
  void cleanup() {
    try (AdminClient adminClient = AdminClient.create(kafkaConfiguration.getKafkaProperties().buildAdminProperties())) {
      adminClient.deleteTopics(Collections.singletonList(CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES));
    }
  }

  @Test
  void corruptedMessageResultsInErrorTaskOfCorruptedType() throws Exception {
    // send corrupted message
    kafkaTemplate.send(
        new ProducerRecord<>(
            CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES,
            null,
            System.currentTimeMillis(),
            "key",
            "{\"corrupted json."
        )
    );

    // send consistent message
    kafkaTemplate.send(
        new ProducerRecord<>(
            CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES,
            null,
            System.currentTimeMillis(),
            "key",
            "{}"
        )
    );

    // there is a message of corrupted type in error state (because no handler provided for it)
    List<Task> corruptedTasks = Awaitility.await().until(
        () -> testTasksService.getTasks(DEFAULT_CORRUPTED_MESSAGE_TASK_TYPE, null, TaskStatus.ERROR),
        tasks -> tasks.size() == 1
    );

    // the corrupted message is properly wrapped and saved
    CorruptedKafkaMessage message = objectMapper.readValue(corruptedTasks.get(0).getData(), CorruptedKafkaMessage.class);
    assertEquals(CorruptedMessageTestSetup.KAFKA_TOPIC_WITH_CORRUPTED_MESSAGES, message.getTopic());
    assertEquals("{\"corrupted json.", message.getCorruptedData());

    // the further messages in the topic are processed
    Awaitility.await().until(
        () -> testTasksService.getTasks(CorruptedMessageTestSetup.TASK_TYPE, null, TaskStatus.DONE),
        tasks -> tasks.size() == 1
    );
  }
}
