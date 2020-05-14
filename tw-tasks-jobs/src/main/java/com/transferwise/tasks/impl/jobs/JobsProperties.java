package com.transferwise.tasks.impl.jobs;

import com.transferwise.tasks.buckets.IBucketsManager;
import lombok.Data;

@Data
@SuppressWarnings("checkstyle:MagicNumber")
public class JobsProperties {

  private String taskTypePrefix = "TaskJob";

  private int concurrency = 10;

  private String processingBucket = IBucketsManager.DEFAULT_ID;

  private boolean testMode = false;
}
