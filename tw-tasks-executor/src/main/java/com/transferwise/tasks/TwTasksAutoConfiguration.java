package com.transferwise.tasks;

import com.transferwise.common.gracefulshutdown.GracefulShutdowner;
import com.transferwise.tasks.config.TwTasksCoreAutoConfiguration;
import com.transferwise.tasks.ext.incidents.config.TwTasksExtIncidentsAutoConfiguration;
import com.transferwise.tasks.ext.kafkalistener.config.TwTasksExtKafkaListenerAutoConfiguration;
import com.transferwise.tasks.ext.kafkapublisher.config.TwTasksExtKafkaPublisherAutoConfiguration;
import com.transferwise.tasks.ext.management.config.TwTasksExtManagementAutoConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(
    {
        TwTasksCoreAutoConfiguration.class,
        TwTasksExtIncidentsAutoConfiguration.class,
        TwTasksExtKafkaListenerAutoConfiguration.class,
        TwTasksExtKafkaPublisherAutoConfiguration.class,
        TwTasksExtManagementAutoConfiguration.class
    }
)
public class TwTasksAutoConfiguration {

  // Following is not used by the code, but makes sure, that someone has not turned graceful shutdown completely off.
  @Autowired
  private GracefulShutdowner gracefulShutdowner;
}
