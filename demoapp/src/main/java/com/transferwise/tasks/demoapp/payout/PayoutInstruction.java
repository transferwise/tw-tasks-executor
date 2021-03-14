package com.transferwise.tasks.demoapp.payout;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class PayoutInstruction {

  private Long id;
  private String payload;
  private String type;
  private Integer priority;

  private String noise;
}
