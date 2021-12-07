## Database performance tests

Allows to test a datastore and tw-tasks throughput and find possible bottlenecks.

Some experience on following can be helpful when getting stuck

- Databases monitoring and profiling
- Java profilers
- Gradle
- Spring Boot
- Junit Jupiter
- Docker, Docker compose

### How-to run locally against external Postgres database

#### Create a database somewhere

```postgresql
create schema demoapp;
```

The tables will be later created automatically by Liquibase.

#### Start Demo App

###### Docker Compose
Tw-tasks needs a Zookeeper and a Kafka cluster. However, those are no bottlenecks by any means, so can be just run in Docker.

```shell
cd demoapp/docker
docker-compose up
```

> Setting `"userland-proxy": false` in docker daemon config is still a good idea for max performance.

###### Demo App configuration

We also need to configure where the database is. The easiest is to create a new spring profile config file, for
example `demoapp/src/main/resources/application-ext-database.yaml`.

```yaml
spring:
  profiles: ext-database
  datasource:
    url: jdbc:postgres://postgres-perf-test.cluster-abc123.eu-central-1.rds.amazonaws.com/demoapp
    password: changeme
    username: changeme
```

###### Running Demo App

Move back to repository root and start the application.

```shell
cd <ROOT>
./gradlew demoapp:bootRun -Dspring.profiles.active=node1,postgres,ext-database
```

To verify it works, one can run `curl localhost:12222/actuator/health` and verify "200" response.

#### Run tests

Comment out `@Disabled` annotation in `integration-tests/src/test/java/com/transferwise/tasks/demoapp/DemoAppRealTest.java`.

Run
```shell
IN_CIRCLE=true ./gradlew :integration-tests:test --tests "com.transferwise.tasks.demoapp.DemoAppRealTest.dbPerfTest"
```

> This will basically run 1 million tasks through, executing `com.transferwise.tasks.demoapp.DemoAppRealTest.dbPerfTest`.
> "IN_CIRCLE=true" prevents creating another local docker environment used for other local tests.

>You may want to truncate following tables at the beginning of each test to get more comparable results.
>```postgresql
>truncate tw_task;
>truncate tw_task_data;
>truncate unique_tw_task_key;
>```

#### Verify test results

Make sure all tasks got executed successfully.

Following query should return 1 million.
```postgresql
select count(*) from tw_task where status='DONE';
```

You can verify how long it took to execute all of them with following.

Mysql family:
```mysql
select TIMESTAMPDIFF(SECOND, min(time_created), max(state_time)) from tw_task where type='DB_PERF_TEST';
```

Postgresql:
```postgresql
select EXTRACT(EPOCH FROM max(state_time)) - EXTRACT(EPOCH FROM min(tw_task.time_created)) from tw_task where type='DB_PERF_TEST';
```

#### Tips and tricks

You can add your exotic database drivers to `demoapp/build.gradle`.

One can also run multiple demo apps in a cluster. For example you can start one with `node1` spring boot profile and
the other with `node2`. In that case, make sure the `com.transferwise.tasks.demoapp.DemoAppRealTest.exchange` is
sending tasks to both of them.

```java
int port = (id % 2 == 0) ? 12222 : 12223;
```