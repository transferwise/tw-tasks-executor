package com.transferwise.tasks.impl.jobs.test;

import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@AutoConfigureBefore(name = "com.transferwise.tasks.core.autoconfigure.TwTasksCoreAutoConfiguration")
@AutoConfigureAfter(name = "com.transferwise.tasks.test.TwTasksCoreTestAutoConfiguration")
@Import(JobsTestConfiguration.class)
public class TwTasksJobsTestAutoConfiguration {

}
