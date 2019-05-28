package com.transferwise.tasks.testappa.postgres

import com.transferwise.tasks.testappa.TaskProcessingIntSpec
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles(value = ["postgres"])
class TaskProcessingPostgresIntSpec extends TaskProcessingIntSpec {
}
