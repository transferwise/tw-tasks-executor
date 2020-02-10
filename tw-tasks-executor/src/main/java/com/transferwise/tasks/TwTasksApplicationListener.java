package com.transferwise.tasks;

import com.transferwise.tasks.utils.ClientIdUtils;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.boot.context.event.ApplicationPreparedEvent;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.env.PropertySource;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.stereotype.Component;

@Component
@FeatureBloat
public class TwTasksApplicationListener implements ApplicationListener<ApplicationEvent> {

  @Override
  public void onApplicationEvent(ApplicationEvent event) {
    if (event instanceof ApplicationPreparedEvent) {
      ApplicationPreparedEvent ape = (ApplicationPreparedEvent) event;

      Boolean allowDurationConfig = ape.getApplicationContext().getEnvironment().getProperty("tw-tasks.core.allow-durations-config", Boolean.class);
      if (Boolean.TRUE.equals(allowDurationConfig)) {
        FormattingConversionService conversionService = new DefaultFormattingConversionService();
        new DateTimeFormatterRegistrar().registerFormatters(conversionService);
        ape.getApplicationContext().getBeanFactory().setConversionService(conversionService);
      }
    } else if (event instanceof ApplicationEnvironmentPreparedEvent) {
      ApplicationEnvironmentPreparedEvent aepe = (ApplicationEnvironmentPreparedEvent) event;

      //noinspection unchecked
      aepe.getEnvironment().getPropertySources().addFirst(new PropertySource<Object>("tw-tasks", new Object()) {
        private volatile String clientIdFromHostname;

        @Override
        public Object getProperty(String name) {
          if (!name.startsWith("tw-tasks.core")) {
            return null;
          } else if (name.endsWith("clientIdFromHostname")) {
            if (clientIdFromHostname == null) {
              synchronized (this) {
                if (clientIdFromHostname == null) {
                  clientIdFromHostname = ClientIdUtils.clientIdFromHostname();
                }
              }
            }
            return clientIdFromHostname;
          } else {
            return null;
          }
        }
      });
    }
  }
}
