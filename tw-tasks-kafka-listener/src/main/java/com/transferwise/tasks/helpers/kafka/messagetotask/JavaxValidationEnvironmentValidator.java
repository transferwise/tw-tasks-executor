package com.transferwise.tasks.helpers.kafka.messagetotask;

import com.transferwise.tasks.helpers.kafka.TwTasksKafkaListenerProperties;
import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;

public class JavaxValidationEnvironmentValidator implements IEnvironmentValidator {

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
