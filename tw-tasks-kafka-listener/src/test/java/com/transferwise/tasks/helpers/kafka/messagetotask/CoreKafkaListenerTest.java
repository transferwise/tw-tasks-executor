package com.transferwise.tasks.helpers.kafka.messagetotask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.transferwise.tasks.TasksProperties;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CoreKafkaListenerTest {

  @Mock
  private TasksProperties tasksProperties;

  @Mock
  private KafkaMessageHandlerRegistry<?> kafkaMessageHandlerRegistry;

  @InjectMocks
  private CoreKafkaListener<?> coreKafkaListener;

  @ParameterizedTest
  @MethodSource("casesForPrefixesFormattingTest")
  void topicPrefixesAreCorrectlyRemoved(String namespace, String topic, String nakedTopic) {
    when(tasksProperties.getKafkaTopicsNamespace()).thenReturn(namespace);
    when(tasksProperties.getKafkaDataCenterPrefixes()).thenReturn("aws.,fra.");
    coreKafkaListener.init();

    String result = coreKafkaListener.removeTopicPrefixes(topic);

    assertEquals(nakedTopic, result);
  }

  private static Stream<Arguments> casesForPrefixesFormattingTest() {
    return Stream.of(
        Arguments.of("", "MyTopic", "MyTopic"),
        Arguments.of("", "fra.MyTopic", "MyTopic"),
        Arguments.of("dev", "dev.MyTopic", "MyTopic"),
        Arguments.of("dev", "dev.fra.MyTopic", "MyTopic")
    );
  }
}
