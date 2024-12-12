package com.transferwise.tasks.triggering;

import com.transferwise.tasks.domain.BaseTask;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.kafka.common.TopicPartition;
import java.time.Instant;

@Data
@Accessors(chain = true)
public class TaskTriggering {

  private BaseTask task;
  private long offset;
  private Instant triggerAt;
  private long sequence;
  private TopicPartition topicPartition;
  private String bucketId;

  public boolean isSameProcessTrigger() {
    return topicPartition == null;
  }
}
