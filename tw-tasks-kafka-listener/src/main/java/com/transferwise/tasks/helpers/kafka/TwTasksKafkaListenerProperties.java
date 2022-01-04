package com.transferwise.tasks.helpers.kafka;

import com.transferwise.tasks.config.ResolvedValue;
import javax.validation.constraints.NotBlank;
import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class TwTasksKafkaListenerProperties {

  /**
   * When we lose the offset of a topic, where do we rewind?
   *
   * <p>Can use "earliest", "latest" or Duration notion. For example, if you want to rewind 30 min back, you should write "-PT30M";
   */
  @NotBlank
  @ResolvedValue
  private String autoResetOffsetTo = "-PT1H";

}
