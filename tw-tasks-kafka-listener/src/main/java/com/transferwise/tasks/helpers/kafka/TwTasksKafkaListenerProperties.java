package com.transferwise.tasks.helpers.kafka;

import com.transferwise.common.baseutils.validation.LegacyResolvedValue;
import com.transferwise.common.baseutils.validation.ResolvedValue;
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
  @jakarta.validation.constraints.NotBlank
  @ResolvedValue
  @LegacyResolvedValue
  private String autoResetOffsetTo = "-PT1H";

}
