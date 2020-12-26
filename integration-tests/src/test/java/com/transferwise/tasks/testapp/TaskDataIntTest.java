package com.transferwise.tasks.testapp;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Strings;
import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.CompressionAlgorithm;
import com.transferwise.tasks.ITasksService.AddTaskRequest;
import com.transferwise.tasks.ITasksService.AddTaskRequest.CompressionRequest;
import com.transferwise.tasks.ITasksService.AddTaskResponse;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDaoDataSerializer.SerializedData;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.test.ITestTasksService;
import com.transferwise.tasks.test.dao.ITestTaskDao;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

public class TaskDataIntTest extends BaseIntTest {

  @Autowired
  private ITestTasksService testTasksService;
  @Autowired
  private ITestTaskDao testTaskDao;
  @Autowired
  private ITaskDao taskDao;
  @Autowired
  private TasksProperties tasksProperties;

  private CompressionAlgorithm originalAlgorithm;
  private int originalMinSize;

  @BeforeEach
  public void setup() {
    originalAlgorithm = tasksProperties.getCompression().getAlgorithm();
    originalMinSize = tasksProperties.getCompression().getMinSize();
    tasksProperties.getCompression().setAlgorithm(CompressionAlgorithm.GZIP);
    tasksProperties.getCompression().setMinSize(100);
  }

  @AfterEach
  public void cleanup() {
    tasksProperties.getCompression().setAlgorithm(originalAlgorithm);
    tasksProperties.getCompression().setMinSize(originalMinSize);
  }

  private static Stream<Arguments> compressionWorksInput() {
    String shortData = "Hello World!";
    String mediumData = Strings.repeat(shortData, 6);
    String longData = Strings.repeat(shortData, 10);
    return Stream.of(
        Arguments.of(shortData, null, 0, shortData.getBytes(StandardCharsets.UTF_8)),
        Arguments.of(longData, null, 1, new byte[]{31, -117, 8, 0, 0, 0, 0, 0, 0, 0}),
        Arguments.of(mediumData, "GZIP", 1, new byte[]{31, -117, 8, 0, 0, 0, 0, 0, 0, 0}),
        Arguments.of(mediumData, "LZ4", 2, new byte[]{76, 90, 52, 66, 108, 111, 99, 107, 38, 22}),
        Arguments.of(shortData, "GZIP", 0, shortData.getBytes(StandardCharsets.UTF_8)),
        Arguments.of(shortData, "LZ4", 0, shortData.getBytes(StandardCharsets.UTF_8)),
        Arguments.of(shortData, "NONE", 0, shortData.getBytes(StandardCharsets.UTF_8)),
        Arguments.of(longData, "NONE", 0, longData.getBytes(StandardCharsets.UTF_8))
    );
  }

  @ParameterizedTest
  @MethodSource("compressionWorksInput")
  void compressionWorks(String originalData, String algorithm, int expectedFormat, byte[] expectedData) {
    testTasksService.stopProcessing();

    byte[] originalDataBytes = originalData.getBytes(StandardCharsets.UTF_8);

    AddTaskRequest addTaskRequest = new AddTaskRequest().setData(originalDataBytes).setType("compressor");
    if (algorithm != null) {
      addTaskRequest.setCompression(new CompressionRequest().setAlgorithm(CompressionAlgorithm.valueOf(algorithm)));
    }
    AddTaskResponse addTaskResponse = testTasksService.addTask(addTaskRequest);
    UUID taskId = addTaskResponse.getTaskId();
    SerializedData serializedData = testTaskDao.getSerializedData(taskId);

    assertThat(serializedData).isNotNull();
    assertThat(serializedData.getDataFormat()).isEqualTo(expectedFormat);
    assertThat(serializedData.getData()).startsWith(expectedData);

    // Deserialization works.
    assertThat(taskDao.getTask(taskId, FullTaskRecord.class).getData()).isEqualTo(originalDataBytes);
    assertThat(taskDao.getTask(taskId, Task.class).getData()).isEqualTo(originalDataBytes);
  }
}
