#ChangeLog

#### 1.5.0 - 2020/04/20
ExponentialTaskRetryPolicy is now handling arithmetic overflows.
But for that, the multiplier was refactored from double to integer.

#### 1.4.0 - 2020/04/20
TwTasksManagement API has a getTask endpoint.

You can now secure all tasks management endpoints by specifying 
`TasksProperties.TasksManagement.viewTaskDataRoles`