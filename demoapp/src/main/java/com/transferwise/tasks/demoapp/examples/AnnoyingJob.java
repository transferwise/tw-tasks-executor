package com.transferwise.tasks.demoapp.examples;

import com.transferwise.tasks.domain.ITask;
import com.transferwise.tasks.impl.jobs.interfaces.IJob;
import com.transferwise.tasks.impl.jobs.interfaces.IJob.ProcessResult.ResultCode;
import java.time.ZonedDateTime;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AnnoyingJob implements IJob {

  @Override
  public ZonedDateTime getNextRunTime() {
    return ZonedDateTime.now().plusMinutes(30);
  }

  @Override
  public ProcessResult process(ITask task) {
    log.info("I'm annoying.");
    return new ProcessResult().setResultCode(ResultCode.SUCCESS);
  }
}
