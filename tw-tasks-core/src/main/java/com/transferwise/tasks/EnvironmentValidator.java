package com.transferwise.tasks;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;

public class EnvironmentValidator extends BaseEnvironmentValidator {

  @Autowired
  private Validator validator;

  @Override
  public void validate() {
    var violations = validator.validate(tasksProperties);
    if (!violations.isEmpty()) {
      throw new ConstraintViolationException(violations);
    }

    super.validate();
  }
}
