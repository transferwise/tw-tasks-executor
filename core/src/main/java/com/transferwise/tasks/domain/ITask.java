package com.transferwise.tasks.domain;

public interface ITask extends IBaseTask {

  String getType();

  String getSubType();

  String getData();

  String getStatus();

  long getProcessingTriesCount();
}
