package com.transferwise.tasks.handler.interfaces;

public enum StuckDetectionSource {
  /**
   * We found `PROCESSING` tasks with the same `processing_client_id` as our `node-id` configuration.
   */
  SAME_NODE_STARTUP,
  /**
   * The normal cluster wide stuck tasks detector.
   */
  CLUSTER_WIDE_STUCK_TASKS_DETECTOR
}
