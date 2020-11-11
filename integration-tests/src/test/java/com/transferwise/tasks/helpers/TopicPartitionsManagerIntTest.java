package com.transferwise.tasks.helpers;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.transferwise.tasks.BaseIntTest;
import com.transferwise.tasks.config.TwTasksKafkaConfiguration;
import com.transferwise.tasks.helpers.kafka.ITopicPartitionsManager;
import com.transferwise.tasks.helpers.kafka.NoOpTopicPartitionsManager;
import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import org.apache.kafka.clients.admin.AdminClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.support.ObjectNameManager;

public class TopicPartitionsManagerIntTest extends BaseIntTest {

  @Autowired
  private ITopicPartitionsManager noOpTopicPartitionsManager;
  @Autowired
  private TwTasksKafkaConfiguration kafkaConfiguration;

  @Test
  public void testAdminClientMbeanId() throws MalformedObjectNameException {
    assertTrue(noOpTopicPartitionsManager instanceof NoOpTopicPartitionsManager);
    try (AdminClient adminClient = noOpTopicPartitionsManager.createKafkaAdminClient(kafkaConfiguration.getKafkaProperties())) {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      String objectName = "kafka.admin.client:type=app-info,id=adminclient-*";
      Set<ObjectInstance> objectInstances = mbeanServer.queryMBeans(ObjectNameManager.getInstance(objectName), null);
      assertTrue(objectInstances.size() >= 1);
    }
  }

}
