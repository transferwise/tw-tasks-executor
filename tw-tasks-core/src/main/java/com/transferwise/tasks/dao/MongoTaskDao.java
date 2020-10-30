package com.transferwise.tasks.dao;

import static com.transferwise.tasks.domain.MongoTask.DATA;
import static com.transferwise.tasks.domain.MongoTask.ID;
import static com.transferwise.tasks.domain.MongoTask.NEXT_EVENT_TIME;
import static com.transferwise.tasks.domain.MongoTask.PRIORITY;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_CLIENT_ID;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_START_TIME;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_TRIES_COUNT;
import static com.transferwise.tasks.domain.MongoTask.STATE_TIME;
import static com.transferwise.tasks.domain.MongoTask.STATUS;
import static com.transferwise.tasks.domain.MongoTask.SUB_TYPE;
import static com.transferwise.tasks.domain.MongoTask.TASK_KEY;
import static com.transferwise.tasks.domain.MongoTask.TASK_KEY_HASH;
import static com.transferwise.tasks.domain.MongoTask.TIME_UPDATED;
import static com.transferwise.tasks.domain.MongoTask.TYPE;
import static com.transferwise.tasks.domain.MongoTask.VERSION;
import static com.transferwise.tasks.utils.MongoTaskUtils.selectOnlyFields;

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.transferwise.common.baseutils.UuidUtils;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.MongoTask;
import com.transferwise.tasks.domain.BaseTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import com.transferwise.tasks.utils.MongoTaskUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
public class MongoTaskDao implements ITaskDao {

  private final MongoTemplate mongoTemplate;
  private final TasksProperties tasksProperties;

  private final String twTaskCollectionName;

  public MongoTaskDao(MongoTemplate mongoTemplate, TasksProperties tasksProperties) {
    this.mongoTemplate = mongoTemplate;
    this.tasksProperties = tasksProperties;

    this.twTaskCollectionName = tasksProperties.getTaskTableName();
    if (!mongoTemplate.collectionExists(twTaskCollectionName)) {
      mongoTemplate.createCollection(twTaskCollectionName);
    }
  }

  @Override
  public ZonedDateTime getEarliestTaskNextEventTime(TaskStatus status) {
    Query query = Query.query(Criteria.where(STATUS).is(status.name()))
        .with(Sort.by(Sort.Direction.ASC, NEXT_EVENT_TIME))
        .limit(1);

    query.fields().include(NEXT_EVENT_TIME);
    Instant nextEventTime = mongoTemplate.findOne(query, Instant.class, twTaskCollectionName);
    return nextEventTime != null ? nextEventTime.atZone(ZoneOffset.UTC) : null;
  }

  @Override
  @Transactional
  public List<StuckTask> prepareStuckOnProcessingTasksForResuming(String clientId, ZonedDateTime maxStuckTime) {
    Instant now = now();
    Instant nextEventTimeFrom = now().minus(tasksProperties.getStuckTaskAge().multipliedBy(2));
    List<StuckTask> result = new ArrayList<>();

    Query stuckOnProcessingQuery = Query.query(Criteria.where(STATUS)
        .is(TaskStatus.PROCESSING)
        .and(NEXT_EVENT_TIME).gt(nextEventTimeFrom)
        .and(PROCESSING_CLIENT_ID).is(clientId));
    selectOnlyFields(stuckOnProcessingQuery, ID, VERSION, TYPE, PRIORITY);

    mongoTemplate.executeQuery(stuckOnProcessingQuery, twTaskCollectionName, (doc) -> {
      StuckTask task = MongoTaskUtils.mapDocToStuckTask(doc);
      long currentVersion = task.getVersionId().getVersion();

      Query updateQuery = Query.query(Criteria.where(ID).is(task.getVersionId().getId())
          .and(VERSION).is(currentVersion));
      Update update = Update.update(STATUS, TaskStatus.SUBMITTED)
          .set(NEXT_EVENT_TIME, maxStuckTime.toInstant())
          .set(STATE_TIME, now)
          .set(TIME_UPDATED, now)
          .set(VERSION, currentVersion + 1);

      UpdateResult updateResult = mongoTemplate.updateFirst(updateQuery, update, twTaskCollectionName);
      if (updateResult.getModifiedCount() == 1) {
        task.getVersionId().setVersion(currentVersion + 1);
        result.add(task);
      }
    });

    return result;
  }

  @Override
  public GetStuckTasksResponse getStuckTasks(int batchSize, TaskStatus status) {
    Query query = Query.query(
        Criteria.where(STATUS).is(status.name())
            .and(NEXT_EVENT_TIME).lt(now()))
        .with(Sort.by(NEXT_EVENT_TIME))
        .limit(batchSize + 1);
    selectOnlyFields(query, ID, VERSION, TYPE, PRIORITY, STATUS);

    List<StuckTask> stuckTasks = executeQueryForTask(query, StuckTask.class);
    boolean hasMore = stuckTasks.size() > batchSize;
    if (hasMore) {
      stuckTasks.remove(stuckTasks.size() - 1);
    }
    return new GetStuckTasksResponse().setStuckTasks(stuckTasks).setHasMore(hasMore);
  }

  @Override
  public InsertTaskResponse insertTask(InsertTaskRequest request) {
    Instant now = now();
    ZonedDateTime nextEventTime = request.getRunAfterTime() == null ? request.getMaxStuckTime() : request.getRunAfterTime();

    String key = request.getKey();
    boolean keyProvided = key != null;
    Integer keyHash = keyProvided ? key.hashCode() : null;
    boolean uuidProvided = request.getTaskId() != null;
    UUID taskId = uuidProvided ? request.getTaskId() : UuidUtils.generatePrefixCombUuid();

    Criteria findExistingTaskCriteria = Criteria.where(ID).is(taskId);
    if (keyProvided) {
      Criteria taskKeyQuery = Criteria.where(TASK_KEY).is(key).and(TASK_KEY_HASH).is(keyHash);
      findExistingTaskCriteria = uuidProvided ? findExistingTaskCriteria.andOperator(taskKeyQuery) : taskKeyQuery;
    }

    Query query = Query.query(findExistingTaskCriteria);
    long savedTaskCount = mongoTemplate.count(query, twTaskCollectionName);
    if (savedTaskCount > 0) {
      log.debug("Task with id '{}' already exist", taskId);
      return new InsertTaskResponse().setInserted(false);
    }

    MongoTask twTask = new MongoTask().setId(taskId)
        .setKey(key)
        .setKeyHash(keyHash)
        .setType(request.getType())
        .setSubType(request.getSubType())
        .setStatus(request.getStatus())
        .setData(request.getData())
        .setNextEventTime(nextEventTime.toInstant())
        .setStateTime(now)
        .setTimeCreated(now)
        .setTimeUpdated(now)
        .setProcessingTriesCount(0)
        .setVersion(0L)
        .setPriority(request.getPriority());
    boolean inserted = false;
    try {
      mongoTemplate.insert(twTask, twTaskCollectionName);
      inserted = true;
    } catch (Throwable t) {
      log.debug("Error occurred inserting task with id '{} and key '{}", taskId, key, t);
    }

    return new InsertTaskResponse()
        .setTaskId(taskId)
        .setInserted(inserted);
  }


  @Override
  public int getTasksCountInStatus(int maxCount, TaskStatus status) {
    Query query = Query.query(Criteria.where(STATUS).is(status)).limit(maxCount);
    return (int) mongoTemplate.count(query, twTaskCollectionName);
  }

  @Override
  public List<Pair<String, Integer>> getTasksCountInErrorGrouped(int maxCount) {
    String typeCount = "type_count";
    Aggregation aggregation = Aggregation.newAggregation(
        Aggregation.match(Criteria.where(STATUS).is(TaskStatus.ERROR)),
        Aggregation.sort(Sort.by(NEXT_EVENT_TIME)),
        Aggregation.limit(maxCount),
        Aggregation.group(TYPE).count().as(typeCount)
    );

    AggregationResults<Document> result = mongoTemplate.aggregate(aggregation, twTaskCollectionName, Document.class);
    return result.getMappedResults().stream()
        .map(e -> {
          String left = e.getString(ID);
          Integer right = e.getInteger(typeCount);
          return ImmutablePair.of(left, right);
        })
        .collect(Collectors.toList());
  }

  @Override
  public int getStuckTasksCount(ZonedDateTime age, int maxCount) {
    Instant ageInst = age.toInstant();

    Function<TaskStatus, Query> stuckTaskQueryFunc = (TaskStatus status) -> {
      Query query = Query.query(Criteria.where(STATUS).is(status).and(NEXT_EVENT_TIME).lt(ageInst))
          .with(Sort.by(NEXT_EVENT_TIME));
      selectOnlyFields(query, ID, VERSION, NEXT_EVENT_TIME);
      return query;
    };

    return (int) Arrays.stream(STUCK_STATUSES)
        .map(stuckTaskQueryFunc)
        .mapToLong(query -> mongoTemplate.count(query, twTaskCollectionName))
        .sum();
  }

  @Override
  public <T> T getTask(UUID taskId, Class<T> clazz) {
    if (taskId == null) {
      return null;
    }
    Query query = Query.query(Criteria.where(ID).is(taskId));
    if (clazz.equals(BaseTask1.class)) {
      selectOnlyFields(query, ID, VERSION, TYPE, STATUS, PRIORITY);
    } else if (clazz.equals(Task.class)) {
      selectOnlyFields(query, ID, VERSION, TYPE, STATUS, PRIORITY, SUB_TYPE, DATA, PROCESSING_TRIES_COUNT);
    } else if (clazz.equals(FullTaskRecord.class)) {
      selectOnlyFields(query, ID, VERSION, TYPE, STATUS, PRIORITY, SUB_TYPE, DATA, PROCESSING_TRIES_COUNT, STATE_TIME, NEXT_EVENT_TIME,
          PROCESSING_CLIENT_ID);
    }

    List<T> results = executeQueryForTask(query, clazz);
    return !results.isEmpty() ? results.get(0) : null;
  }

  @Override
  public DeleteFinishedOldTasksResult deleteOldTasks(TaskStatus taskStatus, Duration age, int batchSize) {
    Instant deletedBeforeTime = now().minus(age);
    Criteria criteria = Criteria.where(STATUS).is(taskStatus)
        .and(NEXT_EVENT_TIME).lt(deletedBeforeTime);
    Query query = Query.query(criteria)
        .with(Sort.by(NEXT_EVENT_TIME))
        .limit(batchSize);
    selectOnlyFields(query, ID, NEXT_EVENT_TIME);

    List<MongoTask> deletedTasks = mongoTemplate.findAllAndRemove(query, MongoTask.class, twTaskCollectionName);

    DeleteFinishedOldTasksResult result = new DeleteFinishedOldTasksResult();
    result.setDeletedTasksCount(deletedTasks.size());
    result.setDeletedUniqueKeysCount(deletedTasks.size());
    result.setFoundTasksCount(deletedTasks.size());
    result.setDeletedBeforeTime(deletedBeforeTime.atZone(ZoneOffset.UTC));

    if (!deletedTasks.isEmpty()) {
      MongoTask firstDeletedTask = deletedTasks.get(0);
      result.setFirstDeletedTaskId(firstDeletedTask.getId());
      result.setFirstDeletedTaskNextEventTime(firstDeletedTask.getNextEventTime().atZone(ZoneOffset.UTC));
    }

    return result;
  }

  @Override
  public boolean deleteTask(UUID taskId, long version) {
    Query query = Query.query(Criteria.where(ID).is(taskId).and(VERSION).is(version));
    DeleteResult deleteResult = mongoTemplate.remove(query, twTaskCollectionName);
    return deleteResult.getDeletedCount() > 0;
  }

  @Override
  public boolean clearPayloadAndMarkDone(UUID taskId, long version) {
    Instant now = now();
    Query query = Query.query(Criteria.where(ID).is(taskId).and(VERSION).is(version));
    Update update = Update.update(DATA, "")
        .set(STATUS, TaskStatus.DONE)
        .set(STATE_TIME, now)
        .set(TIME_UPDATED, now)
        .set(VERSION, version + 1);

    UpdateResult updateResult = mongoTemplate.updateFirst(query, update, twTaskCollectionName);
    return updateResult.getModifiedCount() == 1;
  }

  @Override
  public boolean setToBeRetried(UUID id, ZonedDateTime retryTime, long version, boolean resetTriesCount) {
    Instant now = now();
    Query query = Query.query(Criteria.where(ID).is(id).and(VERSION).is(version));
    Update update = Update.update(STATUS, TaskStatus.WAITING)
        .set(NEXT_EVENT_TIME, retryTime.toInstant())
        .set(STATE_TIME, now)
        .set(TIME_UPDATED, now)
        .set(VERSION, version + 1);

    if (resetTriesCount) {
      update.set(PROCESSING_TRIES_COUNT, 0);
    }

    UpdateResult updateResult = mongoTemplate.updateFirst(query, update, twTaskCollectionName);
    return updateResult.getModifiedCount() == 1;
  }

  @Override
  public Task grabForProcessing(BaseTask task, String clientId, Instant maxProcessingEndTime) {
    Instant now = now();

    Query query = Query.query(
        Criteria.where(ID).is(task.getId())
            .and(VERSION).is(task.getVersion())
            .and(STATUS).is(TaskStatus.SUBMITTED)
    );
    Update update = Update.update(PROCESSING_CLIENT_ID, clientId)
        .set(STATUS, TaskStatus.PROCESSING)
        .set(PROCESSING_START_TIME, now)
        .set(NEXT_EVENT_TIME, maxProcessingEndTime)
        .set(STATE_TIME, now)
        .set(TIME_UPDATED, now)
        .set(VERSION, task.getVersion() + 1);
    update.inc(PROCESSING_TRIES_COUNT);

    UpdateResult updateResult = mongoTemplate.updateFirst(query, update, twTaskCollectionName);
    if (updateResult.getModifiedCount() == 0) {
      return null;
    }
    return getTask(task.getId(), Task.class);
  }

  @Override
  public boolean setStatus(UUID taskId, TaskStatus status, long version) {
    Instant now = now();
    return updateStatus(taskId, version, status, now, now);
  }

  private boolean updateStatus(UUID taskId, long version, TaskStatus status, Instant now, Instant nextEventDate) {
    Query query = Query.query(
        Criteria.where(ID).is(taskId)
            .and(VERSION).is(version)
    );
    Update update = Update.update(STATUS, status)
        .set(NEXT_EVENT_TIME, nextEventDate)
        .set(STATE_TIME, now)
        .set(TIME_UPDATED, now)
        .set(VERSION, version + 1);

    UpdateResult updateResult = mongoTemplate.updateFirst(query, update, twTaskCollectionName);
    return updateResult.getModifiedCount() == 1;
  }

  @Override
  public boolean markAsSubmitted(UUID taskId, long version, ZonedDateTime maxStuckTime) {
    return updateStatus(taskId, version, TaskStatus.SUBMITTED, now(), maxStuckTime.toInstant());
  }

  @Override
  public Long getTaskVersion(UUID id) {
    Query query = Query.query(Criteria.where(ID).is(id));
    selectOnlyFields(query, VERSION);
    MongoTask task = mongoTemplate.findOne(query, MongoTask.class, twTaskCollectionName);
    return task != null ? task.getVersion() : null;
  }

  @Override
  public long getApproximateTasksCount() {
    return mongoTemplate.count(new Query(), twTaskCollectionName);
  }

  @Override
  public long getApproximateUniqueKeysCount() {
    return getApproximateTasksCount();
  }

  private Instant now() {
    return ZonedDateTime.now(TwContextClockHolder.getClock()).toInstant();
  }

  private  <T> List<T> executeQueryForTask(Query query, Class<T> returnType) {
    List<T> result = new ArrayList<>();
    mongoTemplate.executeQuery(query, twTaskCollectionName, document -> {
      T task = MongoTaskUtils.mapDocToType(document, returnType);
      result.add(task);
    });
    return result;
  }
}
