package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.transferwise.tasks.helpers.kafka.TwTasksKafkaListenerProperties;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;

public class EnvironmentValidator implements IEnvironmentValidator {

  @Autowired
  private Validator validator;

  @Autowired
  private TwTasksKafkaListenerProperties properties;

  @Override
  public void validate() {
    var violations = validator.validate(properties);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }
  }
}
