package com.transferwise.tasks.testappa.postgres

import com.transferwise.tasks.testappa.TasksManagementIntSpec
import org.springframework.test.context.ActiveProfiles

@ActiveProfiles(value = ["postgres"])
class TasksManagementPostgresIntSpec extends TasksManagementIntSpec {
}
