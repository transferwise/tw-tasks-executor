# Support

## Support for integration tests
For making integration testing easy and fun, the following support classes can be used. They are especially convenient for
tasks with Json payload.

- `IToKafkaTestHelper`
- `ITestTasksService`

Notice, that you need to define those two beans yourself, they are not created via auto configuration.

Because the TwTasks is used for asynchronous logic, you may want to commit your test data, before starting a test. For
that `ITransactionsHelper` can be used.

For very specific and extreme cases a `ITaskDao` can be directly injected and used.

## Support for pure Spring and Grails applications.
Basically a copy-paste of current auto-configuration class has to be done. Our conditionals are Spring Boot based and will not work there.

No out-of-the box copy-paste free support is currently intended for other than Spring-Boot type of applications.
