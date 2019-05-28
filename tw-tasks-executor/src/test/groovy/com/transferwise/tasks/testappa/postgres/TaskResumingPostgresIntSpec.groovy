package com.transferwise.tasks.testappa.postgres

import com.transferwise.tasks.test.BaseIntSpec
import groovy.util.logging.Slf4j
import org.springframework.test.context.ActiveProfiles

@Slf4j
@ActiveProfiles(value = ["postgres"])
class TaskResumingPostgresIntSpec extends BaseIntSpec {
}
