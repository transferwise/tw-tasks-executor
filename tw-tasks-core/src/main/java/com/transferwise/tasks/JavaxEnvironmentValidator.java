package com.transferwise.tasks;

import javax.validation.ConstraintViolationException;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;

public class JavaxEnvironmentValidator extends BaseEnvironmentValidator {

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
