package com.transferwise.tasks.testappa.config;

import com.transferwise.tasks.management.TasksManagementPortController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@SpringBootApplication
@ComponentScan(value = "com.transferwise.tasks", excludeFilters =
    {@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = TasksManagementPortController.class)})
@EnableTransactionManagement
@Slf4j
public class TestApplication {

}
