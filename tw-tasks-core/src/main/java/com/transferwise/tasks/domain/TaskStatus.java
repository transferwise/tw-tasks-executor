package com.transferwise.tasks.domain;

public enum TaskStatus {
  NEW,
  WAITING,
  SUBMITTED,
  PROCESSING,
  DONE,
  ERROR, // Generates alerts
  FAILED, // ERROR is acked, alerts are off
  CANCELLED, // User requested cancellation, no alerts
  UNKNOWN // For metrics, if getting task status is too expensive
}
