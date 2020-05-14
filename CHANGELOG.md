#ChangeLog

#### 1.6.0 - 2020/05/15
The project is split on modules. The tw-tasks-executor artifact is no longer published.
From now on there is a core module and related extensions that can be easily switched on and off. 
The typical library consumer will replace tw-tasks-executor dependency with: 

```
implementation("com.transferwise.tasks:tw-tasks-core-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-incidents-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-kafka-listener-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-kafka-publisher-spring-boot-starter:${twTasksVersion}")
implementation("com.transferwise.tasks:tw-tasks-management-spring-boot-starter:${twTasksVersion}")

testImplementation("com.transferwise.tasks:tw-tasks-kafka-publisher-test:${twTasksVersion}")
testImplementation("com.transferwise.tasks:tw-tasks-core-test:${twTasksVersion}")
```   

Note that _tw-tasks-incidents_ and _tw-tasks-kafka-listener_ are deprecated and soon will be removed
- Build alerting based on exposed metrics instead of using _tw-tasks-incidents_
- Use spring-kafka or another kafka library instead of using _tw-tasks-kafka-listener_   

#### 1.5.0 - 2020/04/20
ExponentialTaskRetryPolicy is now handling arithmetic overflows.
But for that, the multiplier was refactored from double to integer.

#### 1.4.0 - 2020/04/20
TwTasksManagement API has a getTask endpoint.

You can now secure all tasks management endpoints by specifying 
`TasksProperties.TasksManagement.viewTaskDataRoles`