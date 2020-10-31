package com.transferwise.tasks.test.dao;

import com.transferwise.tasks.TasksProperties;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskStatus;
import com.transferwise.tasks.utils.MongoTaskUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.transaction.annotation.Transactional;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.transferwise.tasks.domain.MongoTask.DATA;
import static com.transferwise.tasks.domain.MongoTask.ID;
import static com.transferwise.tasks.domain.MongoTask.PRIORITY;
import static com.transferwise.tasks.domain.MongoTask.PROCESSING_TRIES_COUNT;
import static com.transferwise.tasks.domain.MongoTask.STATUS;
import static com.transferwise.tasks.domain.MongoTask.SUB_TYPE;
import static com.transferwise.tasks.domain.MongoTask.TYPE;
import static com.transferwise.tasks.domain.MongoTask.VERSION;

public class MongoTestTaskDao implements ITestTaskDao {
  private final MongoTemplate mongoTemplate;
  private final TasksProperties tasksProperties;

  public MongoTestTaskDao(MongoTemplate mongoTemplate, TasksProperties tasksProperties) {
    this.mongoTemplate = mongoTemplate;
    this.tasksProperties = tasksProperties;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteTasks(String type, String subType, TaskStatus... statuses) {
    Query query = buildQueryWithTypeSubTypeAndStatuses(type, subType, statuses);
    mongoTemplate.remove(query, tasksProperties.getTaskTableName());
  }

  private Query buildQueryWithTypeSubTypeAndStatuses(String type, String subType, TaskStatus... statuses) {
    Criteria criteria = Criteria.where(TYPE).is(type);
    if (subType != null) {
      criteria = criteria.and(SUB_TYPE).is(subType);
    }
    if (ArrayUtils.isNotEmpty(statuses)) {
      criteria = criteria.and(STATUS).in(Arrays.asList(statuses));
    }
    return Query.query(criteria);
  }

  @Override
  public List<Task> findTasksByTypeSubTypeAndStatus(String type, String subType, TaskStatus... statuses) {
    Query query = buildQueryWithTypeSubTypeAndStatuses(type, subType, statuses);
    MongoTaskUtils.selectOnlyFields(query, ID, TYPE, SUB_TYPE, DATA, STATUS, VERSION, PROCESSING_TRIES_COUNT, PRIORITY);
    List<Task> results = new ArrayList<>();
    mongoTemplate.executeQuery(query, tasksProperties.getTaskTableName(),
        document -> results.add(MongoTaskUtils.mapDocToTask(document)));
    return results;
  }

  @Override
  @Transactional(rollbackFor = Exception.class)
  public void deleteAllTasks() {
    mongoTemplate.remove(new Query(), tasksProperties.getTaskTableName());
  }
}
