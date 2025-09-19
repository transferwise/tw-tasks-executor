package com.transferwise.tasks.core.autoconfigure;

import com.transferwise.tasks.helpers.CoreMetricsTemplate;
import com.wise.common.environment.WiseEnvironment;
import com.wise.common.environment.WiseProfile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

@Slf4j
public class TwTasksEnvironmentPostProcessor implements EnvironmentPostProcessor {

  private static final String PROPERTY_SOURCE_KEY = TwTasksEnvironmentPostProcessor.class.getName();
  static final String TW_OBS_BASE_EXTREMUM_CONFIG_PATH = "transferwise.observability.base.metrics.local-extremum-gauge-names.tw-tasks";

  @Override
  public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
    PropertySource<?> propertySource = environment.getPropertySources().get(PROPERTY_SOURCE_KEY);
    if (propertySource == null) {
      final HashMap<String, Object> map = new HashMap<>();

      // Calculate last minute min/max using tw-observability-base local extremums.
      Set<String> gaugeNames = new HashSet<>();
      gaugeNames.add(CoreMetricsTemplate.GAUGE_METRIC_PROCESSING_ONGOING_TASKS_GRABBINGS_COUNT);
      gaugeNames.add(CoreMetricsTemplate.GAUGE_PROCESSING_RUNNING_TASKS_COUNT);
      gaugeNames.add(CoreMetricsTemplate.GAUGE_PROCESSING_IN_PROGRESS_TASKS_GRABBING_COUNT);
      gaugeNames.add(CoreMetricsTemplate.GAUGE_TASKS_ONGOING_PROCESSINGS_COUNT);

      map.put(TW_OBS_BASE_EXTREMUM_CONFIG_PATH, gaugeNames);

      MapPropertySource mapPropertySource = new MapPropertySource(PROPERTY_SOURCE_KEY, map);
      environment.getPropertySources().addLast(mapPropertySource);

      WiseEnvironment.setDefaultProperties(dsl -> dsl
          .source("tw-tasks-executor")
          .profile(WiseProfile.PRODUCTION)
          .keyPrefix("tw-tasks.core.")
          .set("max-triggers-in-memory", 100_000)
      );
    }
  }
}
