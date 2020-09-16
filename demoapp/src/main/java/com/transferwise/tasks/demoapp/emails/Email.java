package com.transferwise.tasks.demoapp.emails;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Email {

  private Long id;
  
  private String noise;
}
