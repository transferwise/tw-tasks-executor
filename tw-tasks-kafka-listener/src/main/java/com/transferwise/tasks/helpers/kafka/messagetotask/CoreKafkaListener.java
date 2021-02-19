package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.transferwise.common.baseutils.concurrency.IExecutorServicesProvider;
import com.transferwise.common.baseutils.concurrency.ThreadNamingExecutorServiceWrapper;
import com.transferwise.common.context.UnitOfWorkManager;
import com.transferwise.common.gracefulshutdown.GracefulShutdownStrategy;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.helpers.IErrorLoggingThrottler;
import com.transferwise.tasks.helpers.IMeterHelper;
import com.transferwise.tasks.helpers.kafka.ConsistentKafkaConsumer;
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.configuration.TwTasksKafkaConfiguration;
import com.transferwise.tasks.utils.WaitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PostConstruct;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class CoreKafkaListener<T> implements GracefulShutdownStrategy {

  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;
  @Autowired
  private IExecutorServicesProvider executorServicesProvider;
  @Autowired
  private TasksProperties tasksProperties;
  @Autowired
  private IKafkaMessageHandlerRegistry<T> kafkaMessageHandlerRegistry;
  @Autowired
  private ITopicPartitionsManager topicPartitionsManager;
  @Autowired
  private IErrorLoggingThrottler errorLoggingThrottler;
  @Autowired
  private IMeterHelper meterHelper;
  @Autowired
  private UnitOfWorkManager unitOfWorkManager;

  private ExecutorService executorService;
  private boolean shuttingDown;
  private volatile Map<Integer, List<MyTopic>> topicsShards;
  private List<String> kafkaDataCenterPrefixes;
  private final AtomicInteger inProgressPollers = new AtomicInteger();

  /**
   * Remember to set correct number of partitions for all topics here: @ https://octopus.tw.ee/kafka/topic/change . We could do it automatically, but
   * this would make tests a bit slower.
   *
   * <p>Actually it needs to be tested, if automatic config can be sped up or is it only slow on first time.
   */
  @PostConstruct
  public void init() {
    if (kafkaMessageHandlerRegistry.isEmpty()) {
      return;
    }
    kafkaDataCenterPrefixes = Arrays.asList(StringUtils.split(tasksProperties.getKafkaDataCenterPrefixes(), ","));
  }

  protected Map<Integer, List<MyTopic>> getTopicsShards() {
    if (topicsShards == null) {
      synchronized (this) {
        if (topicsShards == null) {
          Map<Integer, List<MyTopic>> result = new HashMap<>();
          for (IKafkaMessageHandler<T> kafkaMessageHandler : kafkaMessageHandlerRegistry.getKafkaMessageHandlers()) {
            for (IKafkaMessageHandler.Topic topic : kafkaMessageHandler.getTopics()) {
              result.computeIfAbsent(topic.getShard(), (ignored) -> new ArrayList<>())
                  .add(new MyTopic().setAddress(topic.getAddress()).setPartitionsCount(topic.getSuggestedPartitionsCount()));
            }
          }

          topicsShards = result;
        }
      }
    }
    return topicsShards;
  }

  public void poll(int shard, List<String> addresses) {
    Map<String, Object> kafkaConsumerProps = kafkaConfiguration.getKafkaProperties().buildConsumerProperties();
    kafkaConsumerProps.put(
        ConsumerConfig.CLIENT_ID_CONFIG, kafkaConsumerProps.getOrDefault(ConsumerConfig.CLIENT_ID_CONFIG, "") + ".tw-tasks.core-listener." + shard);

    inProgressPollers.incrementAndGet();
    try {
      new ConsistentKafkaConsumer<T>().setTopics(addresses)
          .setDelayTimeout(tasksProperties.getGenericMediumDelay())
          .setShouldPollPredicate(() -> true)
          .setShouldFinishPredicate(() -> shuttingDown)
          .setKafkaPropertiesSupplier(() -> kafkaConsumerProps)
          .setRecordConsumer(record -> {
            String nakedTopic = removeTopicPrefixes(record.topic());
            List<IKafkaMessageHandler<T>> kafkaMessageHandlers = kafkaMessageHandlerRegistry.getForTopicOrFail(nakedTopic);
            kafkaMessageHandlers.forEach(kafkaMessageHandler -> kafkaMessageHandler.handle(record));
            meterHelper.registerKafkaCoreMessageProcessing(shard, record.topic());
          })
          .setErrorLoggingThrottler(errorLoggingThrottler)
          .setMeterHelper(meterHelper)
          .setUnitOfWorkManager(unitOfWorkManager)
          .consume();
    } finally {
      inProgressPollers.decrementAndGet();
    }
  }

  protected void addAddresses(List<String> addresses, String topicAddress) {
    addresses.add(getNamespacedTopic(topicAddress));
    for (String dataCenterPrefix : kafkaDataCenterPrefixes) {
      addresses.add(getNamespacedTopic(dataCenterPrefix + topicAddress));
    }
  }

  private String getNamespacedTopic(String topic) {
    if (StringUtils.isNotEmpty(tasksProperties.getKafkaTopicsNamespace())) {
      return tasksProperties.getKafkaTopicsNamespace() + "." + topic;
    }
    return topic;
  }

  protected String removeTopicPrefixes(String topic) {
    if (StringUtils.isNotEmpty(tasksProperties.getKafkaTopicsNamespace())) {
      String namespace = tasksProperties.getKafkaTopicsNamespace() + ".";
      if (topic.startsWith(namespace)) {
        topic = StringUtils.substringAfter(topic, namespace);
      }
    }
    for (String dataCenterPrefix : kafkaDataCenterPrefixes) {
      if (topic.startsWith(dataCenterPrefix)) {
        topic = StringUtils.substringAfter(topic, dataCenterPrefix);
      }
    }
    return topic;
  }

  @Override
  public void applicationStarted() {
    if (kafkaMessageHandlerRegistry.isEmpty()) {
      return;
    }
    executorService = new ThreadNamingExecutorServiceWrapper("core-kafka-listener", executorServicesProvider.getGlobalExecutorService());
    getTopicsShards().forEach((shard, topics) -> executorService.submit(() -> {
      if (tasksProperties.isCoreKafkaListenerTopicsConfiguringEnabled()) {
        for (MyTopic topic : topics) {
          if (!topic.isConfigured() && topic.getPartitionsCount() != null) {
            topicPartitionsManager.setPartitionsCount(getNamespacedTopic(topic.getAddress()), topic.getPartitionsCount());
            topic.setConfigured(true);
          }
        }
      }

      List<String> addresses = new ArrayList<>();
      for (MyTopic topic : topics) {
        addAddresses(addresses, topic.getAddress());
      }

      addresses.forEach(a -> log.info("Listening topic '" + a + "' in shard " + shard + "."));

      while (!shuttingDown) {
        try {
          poll(shard, addresses);
        } catch (Throwable t) {
          log.error(t.getMessage(), t);
          WaitUtils.sleepQuietly(tasksProperties.getGenericMediumDelay());
        }
      }
    }));
  }

  @Override
  public void prepareForShutdown() {
    shuttingDown = true;
  }

  @Override
  public boolean canShutdown() {
    return inProgressPollers.get() == 0;
  }

  @Data
  @Accessors(chain = true)
  private static class MyTopic {

    private String address;
    private boolean configured;
    private Integer partitionsCount;
  }
}
