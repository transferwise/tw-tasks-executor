package com.transferwise.tasks.stucktasks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.dao.ITaskDao.StuckTask;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.domain.TaskVersionId;
import com.transferwise.tasks.handler.interfaces.ITaskHandler;
import com.transferwise.tasks.handler.interfaces.ITaskHandlerRegistry;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy;
import com.transferwise.tasks.handler.interfaces.ITaskProcessingPolicy.StuckTaskResolutionStrategy;
import com.transferwise.tasks.handler.interfaces.StuckDetectionSource;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.triggering.ITasksExecutionTriggerer;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.curator.framework.CuratorFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TasksResumerTest {

  @Mock
  private ITasksExecutionTriggerer tasksExecutionTriggerer;

  @Mock
  private ITaskHandlerRegistry taskHandlerRegistry;

  @Mock
  private TasksProperties tasksProperties;

  @Mock
  private ITaskDao taskDao;

  @Mock
  private CuratorFramework curatorFramework;

  @Mock
  private IExecutorServicesProvider executorServicesProvider;

  @Mock
  private IMeterHelper meterHelper;

  @InjectMocks
  private TasksResumer service;

  private final ZonedDateTime now = ZonedDateTime.now(TwContextClockHolder.getClock());

  @BeforeEach
  void setup() {
    lenient().when(tasksProperties.getTaskStuckTimeout()).thenReturn(Duration.ofMinutes(10));
    TwContextClockHolder.setClock(Clock.fixed(now.toInstant(), now.getZone()));
  }

  @Test
  void stuckTasksNotInProcessingStateWillBeResumed() {
    when(taskDao.markAsSubmitted(any(), anyLong(), any())).thenReturn(true);

    StuckTask task = new ITaskDao.StuckTask()
        .setType("TEST")
        .setStatus(TaskStatus.SUBMITTED.name())
        .setVersionId(new TaskVersionId(UuidUtils.generatePrefixCombUuid(), 0));
    AtomicInteger numResumed = new AtomicInteger();

    service.handleStuckTask(task, StuckDetectionSource.CLUSTER_WIDE_STUCK_TASKS_DETECTOR, numResumed, null, null);

    verify(tasksExecutionTriggerer).trigger(argThat(
        baseTask -> 1 == baseTask.getVersion() && task.getVersionId().getId().equals(baseTask.getId())
    ));
    verify(taskDao).markAsSubmitted(
        task.getVersionId().getId(),
        task.getVersionId().getVersion(),
        now.plusMinutes(10)
    );
  }

  @ParameterizedTest(name = "handleStuckTask respects StuckTaskResolutionStrategy {3}")
  @MethodSource("resolutionCasesForHandleStuckTransfers")
  void handleStuckTasksRespectsStuckTaskResolutionStrategy(
      int resumed,
      int failed,
      int error,
      StuckTaskResolutionStrategy resolutionStrategy
  ) {
    lenient().when(taskDao.markAsSubmitted(any(), anyLong(), any())).thenReturn(true);
    lenient().when(taskDao.setStatus(any(), any(), anyLong())).thenReturn(true);

    ITaskProcessingPolicy processingPolicy = Mockito.mock(ITaskProcessingPolicy.class);
    when(processingPolicy.getStuckTaskResolutionStrategy(any(), any())).thenReturn(resolutionStrategy);
    lenient().when(processingPolicy.getProcessingDeadline(any())).thenReturn(null);

    when(taskHandlerRegistry.getTaskHandler(any())).thenReturn(Mockito.mock(ITaskHandler.class));
    when(taskHandlerRegistry.getTaskHandler(null).getProcessingPolicy(any())).thenReturn(processingPolicy);

    // triggering tasks handling
    AtomicInteger numResumed = new AtomicInteger();
    AtomicInteger numError = new AtomicInteger();
    AtomicInteger numFailed = new AtomicInteger();
    StuckTask task = new ITaskDao.StuckTask()
        .setType("TEST")
        .setStatus(TaskStatus.PROCESSING.name())
        .setVersionId(new TaskVersionId(UuidUtils.generatePrefixCombUuid(), 0));
    service.handleStuckTask(task, StuckDetectionSource.CLUSTER_WIDE_STUCK_TASKS_DETECTOR, numResumed, numError, numFailed);

    verify(tasksExecutionTriggerer, times(resumed)).trigger(argThat(
        baseTask -> baseTask.getVersion() == 1 && baseTask.getId().equals(task.getVersionId().getId())
    ));
    verify(taskDao, times(resumed)).markAsSubmitted(
        task.getVersionId().getId(),
        task.getVersionId().getVersion(),
        now.plusMinutes(10)
    );
    verify(taskDao, times(error)).setStatus(
        task.getVersionId().getId(),
        TaskStatus.ERROR,
        task.getVersionId().getVersion()
    );
    verify(taskDao, times(failed)).setStatus(
        task.getVersionId().getId(),
        TaskStatus.FAILED,
        task.getVersionId().getVersion()
    );
    assertEquals(resumed, numResumed.get());
    assertEquals(error, numError.get());
    assertEquals(failed, numFailed.get());
  }

  private static Stream<Arguments> resolutionCasesForHandleStuckTransfers() {
    return Stream.of(
        Arguments.of(1, 0, 0, StuckTaskResolutionStrategy.RETRY),
        Arguments.of(0, 1, 0, StuckTaskResolutionStrategy.MARK_AS_FAILED),
        Arguments.of(0, 0, 1, StuckTaskResolutionStrategy.MARK_AS_ERROR),
        Arguments.of(0, 0, 0, StuckTaskResolutionStrategy.IGNORE)
    );
  }
}
