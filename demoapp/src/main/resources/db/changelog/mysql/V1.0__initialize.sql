CREATE TABLE tw_task
(
  id                     BINARY(16) PRIMARY KEY NOT NULL,
  status                 ENUM('NEW', 'WAITING', 'SUBMITTED', 'PROCESSING', 'DONE', 'ERROR', 'FAILED', 'CANCELLED'),
  -- Microsecond precision (6) is strongly recommended here to reduce the chance of gap locks deadlocking on tw_task_idx1
  next_event_time        DATETIME(6) NOT NULL,
  state_time             DATETIME(3) NOT NULL,
  version                BIGINT                            NOT NULL,
  priority               INT                               NOT NULL DEFAULT 5,
  processing_start_time  DATETIME(3) NULL,
  processing_tries_count BIGINT                            NOT NULL,
  time_created           DATETIME(3) NOT NULL,
  time_updated           DATETIME(3) NOT NULL,
  type                   VARCHAR(250) CHARACTER SET latin1 NOT NULL,
  sub_type               VARCHAR(250) CHARACTER SET latin1 NULL,
  processing_client_id   VARCHAR(250) CHARACTER SET latin1 NULL,
  data                   LONGTEXT                          NOT NULL
);

CREATE INDEX tw_task_idx1 ON tw_task (status, next_event_time);

CREATE TABLE tw_task_data
(
  task_id     BINARY(16) PRIMARY KEY NOT NULL,
  data_format INT      NOT NULL,
  data        LONGBLOB NOT NULL
);

CREATE TABLE unique_tw_task_key
(
  task_id  BINARY(16) PRIMARY KEY,
  key_hash INT                               NOT NULL,
  `key`    VARCHAR(150) CHARACTER SET latin1 NOT NULL,
  UNIQUE KEY uidx1 (key_hash, `key`)
);