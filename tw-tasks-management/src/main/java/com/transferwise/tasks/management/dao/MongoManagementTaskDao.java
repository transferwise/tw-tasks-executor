package com.transferwise.tasks.management.dao;

import com.mongodb.client.result.UpdateResult;
import com.transferwise.common.context.TwContextClockHolder;
import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.utils.ListUtils;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.transferwise.tasks.dao.ITaskDao.STUCK_STATUSES;
import static com.transferwise.tasks.domain.MongoTask.DATA;
import static com.transferwise.tasks.domain.MongoTask.ID;
import static com.transferwise.tasks.domain.MongoTask.NEXT_EVENT_TIME;
import static com.transferwise.tasks.domain.MongoTask.PRIORITY;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_CLIENT_ID;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_TRIES_COUNT;
import static com.transferwise.tasks.domain.MongoTask.STATE_TIME;
import static com.transferwise.tasks.domain.MongoTask.STATUS;
import static com.transferwise.tasks.domain.MongoTask.SUB_TYPE;
import static com.transferwise.tasks.domain.MongoTask.TIME_UPDATED;
import static com.transferwise.tasks.domain.MongoTask.TYPE;
import static com.transferwise.tasks.domain.MongoTask.VERSION;
import static com.transferwise.tasks.utils.MongoTaskUtils.selectOnlyFields;

public class MongoManagementTaskDao implements IManagementTaskDao {
  private final MongoTemplate mongoTemplate;
  private final TasksProperties tasksProperties;

  private final int[] questionBuckets = {1, 5, 25, 125, 625};
  private static final String[] FULL_TASK_FIELDS = new String[] {
      VERSION, TYPE, SUB_TYPE, DATA, STATUS, VERSION, PROCESSING_TRIES_COUNT, PRIORITY, STATE_TIME, NEXT_EVENT_TIME, PROCESSING_CLIENT_ID
  };

  public MongoManagementTaskDao(MongoTemplate mongoTemplate, TasksProperties tasksProperties) {
    this.mongoTemplate = mongoTemplate;
    this.tasksProperties = tasksProperties;
  }

  @Override
  public List<DaoTask1> getTasksInErrorStatus(int maxCount) {
    Query query = Query.query(Criteria.where(STATUS).is(TaskStatus.ERROR))
        .with(Sort.by(Direction.DESC, NEXT_EVENT_TIME))
        .limit(maxCount);
    selectOnlyFields(query, ID, VERSION, STATE_TIME, TYPE, SUB_TYPE);

    return executeQueryForTask(query, DaoTask1.class);
  }

  @Override
  public List<DaoTask3> getTasksInProcessingOrWaitingStatus(int maxCount) {
    Function<TaskStatus, Query> queryFunction = (status) -> {
      Query query = Query.query(Criteria.where(STATUS).is(status))
          .with(Sort.by(Direction.DESC, NEXT_EVENT_TIME))
          .limit(maxCount);
      selectOnlyFields(query, ID, VERSION, STATE_TIME, NEXT_EVENT_TIME, TYPE, SUB_TYPE, STATUS);
      return query;
    };

    List<DaoTask3> result = Arrays.stream(ITaskDao.WAITING_AND_PROCESSING_STATUSES)
        .map(queryFunction)
        .flatMap(query -> executeQueryForTask(query, DaoTask3.class).stream())
        .sorted(Comparator.comparing(DaoTask3::getNextEventTime).reversed())
        .collect(Collectors.toList());

    return result.subList(0, Math.min(maxCount, result.size()));
  }

  @Override
  public List<FullTaskRecord> getTasks(List<UUID> uuids) {
    return ListUtils.chunkIntoVariableSizeAndOperate(uuids, questionBuckets, (chunkedList, bucketId) -> {
      Query query = Query.query(Criteria.where(ID).in(chunkedList));
      selectOnlyFields(query, FULL_TASK_FIELDS);
      return executeQueryForTask(query, FullTaskRecord.class);
    });
  }

  @Override
  public boolean scheduleTaskForImmediateExecution(UUID taskId, long version) {
    Instant now = Instant.now(TwContextClockHolder.getClock());
    Query updateQuery = Query.query(Criteria.where(ID).is(taskId).and(VERSION).is(version));
    Update update = Update.update(STATUS, TaskStatus.WAITING)
        .set(NEXT_EVENT_TIME, now)
        .set(STATE_TIME, now)
        .set(TIME_UPDATED, now)
        .set(VERSION, version + 1);

    UpdateResult updateResult = mongoTemplate.updateFirst(updateQuery, update, tasksProperties.getTaskTableName());
    return updateResult.getModifiedCount() == 1;
  }

  @Override
  public List<DaoTask2> getStuckTasks(int maxCount, Duration delta) {
    Instant timeThreshold = Instant.now(TwContextClockHolder.getClock()).minus(delta);

    Function<TaskStatus, Query> stuckTaskQueryFunc = (TaskStatus status) -> {
      Query query = Query.query(Criteria.where(STATUS).is(status).and(NEXT_EVENT_TIME).lt(timeThreshold));
      selectOnlyFields(query, ID, VERSION, NEXT_EVENT_TIME);
      return query;
    };

    List<DaoTask2> stuckTasks = Arrays.stream(STUCK_STATUSES)
        .map(stuckTaskQueryFunc)
        .flatMap(query -> executeQueryForTask(query, DaoTask2.class).stream())
        .sorted(Comparator.comparing(DaoTask2::getNextEventTime).reversed())
        .collect(Collectors.toList());

    return stuckTasks.subList(0, Math.min(maxCount, stuckTasks.size()));
  }

  private  <T> List<T> executeQueryForTask(Query query, Class<T> returnType) {
    List<T> result = new ArrayList<>();
    mongoTemplate.executeQuery(query, tasksProperties.getTaskTableName(), document -> {
      T task = MongoTaskToDaoMapper.mapDocToDaoType(document, returnType);
      result.add(task);
    });
    return result;
  }
}
