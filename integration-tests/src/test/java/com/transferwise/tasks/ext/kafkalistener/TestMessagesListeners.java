package com.transferwise.tasks.ext.kafkalistener;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class TestMessagesListeners implements ITestMessagesListener {

  private List<ITestMessagesListener> listeners = new ArrayList<>();

  public void messageReceived(ConsumerRecord<String, String> record) {
    for (ITestMessagesListener listener : listeners) {
      listener.messageReceived(record);
    }
  }

  public void addListener(ITestMessagesListener listener) {
    listeners.add(listener);
  }

  public void removeListener(ITestMessagesListener listener) {
    listeners.remove(listener);
  }
}
