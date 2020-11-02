package com.transferwise.tasks.testapp.dao;

import com.transferwise.tasks.domain.MongoTask;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("mongo")
class MongoTaskDaoIntTest extends TaskDaoIntTest {
  @Autowired
  private MongoTemplate mongoTemplate;

  @Override
  List<String> findDataByType(String taskType) {
    Query query = Query.query(Criteria.where("type").is(taskType));
    query.fields().include("data");
    List<MongoTask> result = mongoTemplate.find(query, MongoTask.class, "tw_task");
    return result.stream().map(MongoTask::getData).collect(Collectors.toList());
  }

  @Override
  int getUniqueTaskKeysCount() {
    return (int) taskDao.getApproximateUniqueKeysCount();
  }
}
