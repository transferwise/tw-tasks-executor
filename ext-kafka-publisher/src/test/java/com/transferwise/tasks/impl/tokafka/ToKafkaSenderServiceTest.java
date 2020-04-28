package com.transferwise.tasks.impl.tokafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.tracing.IXRequestIdHolder;
import com.transferwise.tasks.ITasksService;
import com.transferwise.tasks.impl.tokafka.IToKafkaSenderService.SendMessageRequest;
import com.transferwise.tasks.impl.tokafka.IToKafkaSenderService.SendMessagesRequest;
import com.transferwise.tasks.impl.tokafka.ToKafkaMessages.Message;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ToKafkaSenderServiceTest {

  @Mock
  private ObjectMapper objectMapper;

  @Mock
  private ITasksService taskService;

  @Mock
  private IXRequestIdHolder ixRequestIdHolder;

  private ToKafkaSenderService toKafkaSenderService;

  @BeforeEach
  void setup() {
    toKafkaSenderService = new ToKafkaSenderService(objectMapper, taskService, 8, ixRequestIdHolder);
  }

  @Test
  void payloadIsBeingConverted() throws Exception {
    when(objectMapper.writeValueAsString("abc")).thenReturn("abc");

    toKafkaSenderService.sendMessage(new SendMessageRequest().setPayload("abc"));

    verify(objectMapper).writeValueAsString("abc");
    verify(taskService).addTask(any());
  }

  @Test
  void payloadIsBeingConvertedInCaseOfBatch() throws Exception {
    when(objectMapper.writeValueAsString("abc")).thenReturn("abc");

    toKafkaSenderService.sendMessages(new SendMessagesRequest().add(new SendMessagesRequest.Message().setPayload("abc")));

    verify(objectMapper).writeValueAsString("abc");
    verify(taskService).addTask(any());
  }

  @Test
  void payloadStringIsUsedAsItIs() throws Exception {
    toKafkaSenderService.sendMessage(new SendMessageRequest().setPayloadString("abc"));

    verify(objectMapper, never()).writeValueAsString(anyString());
    verify(taskService).addTask(any());
  }

  @Test
  void payloadStringIsBeingConvertedInCaseOfBatch() throws Exception {
    toKafkaSenderService.sendMessages(new SendMessagesRequest().add(new SendMessagesRequest.Message().setPayloadString("abc")));

    verify(objectMapper, never()).writeValueAsString(anyString());
    verify(taskService).addTask(any());
  }

  @Test
  void messagesAreSplitToBatches() {
    // a huge request
    long millis = System.currentTimeMillis();
    SendMessagesRequest request = new SendMessagesRequest();

    // 1024 strings of 1 KiB
    List<String> randomStrings = IntStream.range(1, 1025)
        .mapToObj(i -> RandomStringUtils.randomAlphabetic(1024))
        .collect(Collectors.toList());

    for (int i = 0; i < 50; i++) {
      Collections.shuffle(randomStrings);
      // 1 MiB ~1 MB
      request.add(new SendMessagesRequest.Message().setPayloadString(String.join("", randomStrings)));
    }
    System.out.printf("Created 50 MB of random strings in %d ms.%n", System.currentTimeMillis() - millis);

    // sending it
    toKafkaSenderService.sendMessages(request);

    verify(taskService, atLeast(8)).addTask(any());
  }

  @Test
  void messagesThatAreBiggerThanBatchSizeAreStillBeingSent() {
    // a huge request
    long millis = System.currentTimeMillis();
    SendMessagesRequest request = new SendMessagesRequest();
    String s1 = RandomStringUtils.randomAlphabetic(1024 * 1024 * 6); // two 6 MiB strings
    String s2 = RandomStringUtils.randomAlphabetic(1024 * 1024 * 6); // two 6 MiB strings
    request.add(new SendMessagesRequest.Message().setPayloadString(s1));
    request.add(new SendMessagesRequest.Message().setPayloadString(s2));
    System.out.printf("Created 2 random 6 MB strings in %d ms.%n", System.currentTimeMillis() - millis);

    // sending it
    toKafkaSenderService.sendMessages(request);

    verify(taskService, times(2)).addTask(any());
  }

  @ParameterizedTest(name = "batches are calculated correctly {0} produces {1}")
  @MethodSource("casesForBatchSizeSplit")
  void batchesAreCalculatedCorrectly(String messageDescriptor, String expectedBatch) {
    int mb = 1024 * 1024;

    List<Message> messages = Arrays.stream(messageDescriptor.split(" "))
        .map(
            pairString -> {
              String[] pair = pairString.split(":");
              int size = Integer.parseInt(pair[1].replace("MB", ""));
              return mockMessage(size * mb, pair[0]);
            }
        )
        .collect(Collectors.toList());

    String batchDescriptor = toKafkaSenderService.splitToBatches(messages, 10 * mb)
        .stream()
        .map(it -> it.stream().map(Message::getMessage).collect(Collectors.joining(", ", "[", "]")))
        .collect(Collectors.joining());

    assertEquals(expectedBatch, batchDescriptor);
  }

  private static Stream<Arguments> casesForBatchSizeSplit() {
    return Stream.of(
        Arguments.of("1:6MB", "[1]"),
        Arguments.of("1:6MB 2:6MB", "[1][2]"),
        Arguments.of("1:6MB 2:4MB", "[1, 2]"),
        Arguments.of("1:6MB 2:6MB 3:12MB", "[1][2][3]"),
        Arguments.of("1:3MB 2:3MB 3:3MB", "[1, 2, 3]"),
        Arguments.of("1:3MB 2:3MB 3:5MB", "[1, 2][3]"),
        Arguments.of("1:11MB", "[1]"),
        Arguments.of("1:11MB 2:11MB", "[1][2]"),
        Arguments.of("1:4MB 2:1MB 3:2MB 4:1MB 5:4MB 6:2MB", "[1, 2, 3, 4][5, 6]"),
        Arguments.of("1:4MB 2:1MB 3:2MB 4:1MB 5:2MB 6:6MB", "[1, 2, 3, 4, 5][6]")
    );
  }

  private static ToKafkaMessages.Message mockMessage(int size, String id) {
    ToKafkaMessages.Message mock = Mockito.mock(ToKafkaMessages.Message.class);
    when(mock.getMessage()).thenReturn(id);
    when(mock.getApproxSize()).thenReturn(size);
    return mock;
  }
}
