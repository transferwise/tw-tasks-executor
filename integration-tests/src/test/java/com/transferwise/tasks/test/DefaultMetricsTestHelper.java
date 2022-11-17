package com.transferwise.tasks.test;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DefaultMetricsTestHelper implements MetricsTestHelper {

  @Autowired
  private MeterRegistry meterRegistry;

  @Override
  public double getCount(String name, String... tags) {
    var counters = meterRegistry.find(name).tags(tags).counters();
    if (counters.isEmpty()) {
      return 0d;
    }
    if (counters.size() > 1) {
      throw new IllegalStateException("" + counters.size() + " counters found for '" + name + "'(" + StringUtils.join(tags, ",") + ")");
    }

    return counters.iterator().next().count();
  }
}
