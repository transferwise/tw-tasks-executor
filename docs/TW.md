## Development and Releasing
In Slack: `#tw-task-exec`

If you change only README file, please add `[ci skip]` to commit message (the description, not the title).

Please ask for code review in `#tw-task-exec`.

CircleCI builds on any change in `master`, version can be found [here](https://github.com/transferwise/tw-tasks-executor/releases).

## Notes
TwTasks was mainly developed to replace our Artemis based Payouts Engine, but also Statements Import Engine with something 
more robust and scalable, which is able to satisfy the following requirements.

## Task management UI in Ninjas2
Just list your service in [here](https://github.com/transferwise/ninjas2/blob/master/src/modules/tools/twtasks/TwTasksManagementController.ts).
In Ninjas2, it can be found under `New Tools` menu.

## Support for SpringBoot-1.4.X or Grails-3.2.9 applications.
You can refer to this [article](https://transferwise.atlassian.net/wiki/spaces/EKB/pages/907872798/SpringBoot+1.4.X+to+work+with+TwTask+SpringBoot+1.5.X+).

## Initial tw-task setup code reference
You can refer to this [pull_request](https://github.com/transferwise/document-review/pull/21/files).

## Useful queries
`WAITING` tasks can be found from looker by:
```
SELECT encode(id, 'escape') || '-' || version
FROM tw_task
WHERE type = 'UpdateLastTransferCountry' AND status = 'WAITING'
```

Finding a task by UUID (note how you need to remove the '-' from the UUID):
```
SELECT HEX(id), version, time_created, status, type, next_event_time, data
FROM payin.tw_task
WHERE id = UNHEX(REPLACE('219c005c-08d8-4a02-9606-6e38e20d0f5c', '-', ''))
```
