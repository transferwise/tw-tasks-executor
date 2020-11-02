package com.transferwise.tasks.management.dao;

import static com.transferwise.tasks.utils.MongoTaskUtils.getId;
import static com.transferwise.tasks.utils.MongoTaskUtils.getNumberValue;
import static com.transferwise.tasks.utils.MongoTaskUtils.getZonedDateForField;

import com.transferwise.tasks.domain.MongoTask;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask1;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask2;
import com.transferwise.tasks.management.dao.IManagementTaskDao.DaoTask3;
import com.transferwise.tasks.utils.MongoTaskUtils;
import org.bson.Document;

public class MongoTaskToDaoMapper {

  public static DaoTask1 mapDocToDaoTask1(Document doc) {
    return new DaoTask1().setId(getId(doc))
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue())
        .setType(doc.getString(MongoTask.TYPE))
        .setSubType(doc.getString(MongoTask.SUB_TYPE))
        .setStateTime(getZonedDateForField(doc, MongoTask.STATE_TIME));
  }

  public static DaoTask2 mapDocToDaoTask2(Document doc) {
    return new DaoTask2().setId(getId(doc))
        .setNextEventTime(getZonedDateForField(doc, MongoTask.NEXT_EVENT_TIME))
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue());
  }

  public static DaoTask3 mapDocToDaoTask3(Document doc) {
    return new DaoTask3().setId(getId(doc))
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue())
        .setStateTime(getZonedDateForField(doc, MongoTask.STATE_TIME))
        .setStatus(doc.getString(MongoTask.STATUS))
        .setType(doc.getString(MongoTask.TYPE))
        .setSubType(doc.getString(MongoTask.SUB_TYPE))
        .setNextEventTime(getZonedDateForField(doc, MongoTask.NEXT_EVENT_TIME));
  }

  @SuppressWarnings("unchecked")
  public static <T> T mapDocToDaoType(Document doc, Class<T> type) {
    if (DaoTask1.class.equals(type)) {
      return (T) mapDocToDaoTask1(doc);
    } else if (DaoTask2.class.equals(type)) {
      return (T) mapDocToDaoTask2(doc);
    } else if (DaoTask3.class.equals(type)) {
      return (T) mapDocToDaoTask3(doc);
    }

    return MongoTaskUtils.mapDocToType(doc, type);
  }
}
