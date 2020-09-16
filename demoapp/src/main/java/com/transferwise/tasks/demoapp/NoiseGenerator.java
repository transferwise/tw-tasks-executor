package com.transferwise.tasks.demoapp;

import org.apache.commons.lang3.RandomStringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class NoiseGenerator {

  @Value("${demoapp.data-noise-amount}")
  private int noiseAmount;

  private String cached;

  public String generateNoise() {
    if (cached != null) {
      return cached;
    }
    return cached = RandomStringUtils.randomAlphabetic(noiseAmount);
  }
}
