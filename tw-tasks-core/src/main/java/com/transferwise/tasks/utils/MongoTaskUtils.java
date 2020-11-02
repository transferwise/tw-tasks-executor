package com.transferwise.tasks.utils;

import com.transferwise.tasks.dao.ITaskDao.StuckTask;
import com.transferwise.tasks.domain.BaseTask1;
import com.transferwise.tasks.domain.FullTaskRecord;
import com.transferwise.tasks.domain.MongoTask;
import com.transferwise.tasks.domain.Task;
import com.transferwise.tasks.domain.TaskVersionId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;
import lombok.experimental.UtilityClass;
import org.bson.Document;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.data.mongodb.core.query.Query;

@UtilityClass
public class MongoTaskUtils {

  public static StuckTask mapDocToStuckTask(Document doc) {
    return new StuckTask()
        .setVersionId(new TaskVersionId(getId(doc), getNumberValue(doc, MongoTask.VERSION).longValue()))
        .setType(doc.getString(MongoTask.TYPE))
        .setPriority(getNumberValue(doc, MongoTask.PRIORITY).intValue())
        .setStatus(doc.getString(MongoTask.STATUS));
  }

  public static BaseTask1 mapDocToBaseTask1(Document doc) {
    return new BaseTask1().setId(getId(doc))
        .setType(doc.getString(MongoTask.TYPE))
        .setPriority(getNumberValue(doc, MongoTask.PRIORITY).intValue())
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue())
        .setStatus(doc.getString(MongoTask.STATUS));
  }

  public static Task mapDocToTask(Document doc) {
    return new Task().setId(getId(doc))
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue())
        .setStatus(doc.getString(MongoTask.STATUS))
        .setSubType(doc.getString(MongoTask.SUB_TYPE))
        .setData(doc.getString(MongoTask.DATA))
        .setType(doc.getString(MongoTask.TYPE))
        .setPriority(getNumberValue(doc, MongoTask.PRIORITY).intValue())
        .setProcessingTriesCount(getNumberValue(doc, MongoTask.PROCESSING_TRIES_COUNT).longValue());
  }

  public static FullTaskRecord mapDocToFullTaskRecord(Document doc) {
    return new FullTaskRecord().setId(getId(doc))
        .setVersion(getNumberValue(doc, MongoTask.VERSION).longValue())
        .setStatus(doc.getString(MongoTask.STATUS))
        .setType(doc.getString(MongoTask.TYPE))
        .setSubType(doc.getString(MongoTask.SUB_TYPE))
        .setData(doc.getString(MongoTask.DATA))
        .setProcessingTriesCount(getNumberValue(doc, MongoTask.PROCESSING_TRIES_COUNT).longValue())
        .setPriority(getNumberValue(doc, MongoTask.PRIORITY).intValue())
        .setStateTime(getZonedDateForField(doc, MongoTask.STATE_TIME))
        .setNextEventTime(getZonedDateForField(doc, MongoTask.NEXT_EVENT_TIME))
        .setProcessingClientId(doc.getString(MongoTask.PROCESSING_CLIENT_ID));
  }

  @SuppressWarnings("unchecked")
  public static <T> T mapDocToType(Document doc, Class<T> type) {
    if (BaseTask1.class.equals(type)) {
      return (T) mapDocToBaseTask1(doc);
    } else if (Task.class.equals(type)) {
      return (T) mapDocToTask(doc);
    } else if (FullTaskRecord.class.equals(type)) {
      return (T) mapDocToFullTaskRecord(doc);
    } else if (StuckTask.class.equals(type)) {
      return (T) mapDocToStuckTask(doc);
    }

    throw new IllegalStateException("Unsupported class of '" + type.getCanonicalName() + "'.");
  }

  public static ZonedDateTime getZonedDateForField(Document doc, String field) {
    Date date = doc.get(field, Date.class);
    return date != null ? date.toInstant().atZone(ZoneOffset.UTC) : null;
  }

  public static Number getNumberValue(Document doc, String field) {
    Object value = doc.get(field);

    if (value instanceof Number) {
      return (Number) value;
    }
    throw new IllegalStateException("Value from mongo document is not a number");
  }

  public static UUID getId(Document doc) {
    return doc.get(MongoTask.ID, UUID.class);
  }

  public static void selectOnlyFields(Query query, String... fieldsToInclude) {
    Field fields = query.fields();
    Arrays.stream(fieldsToInclude)
        .forEach(fields::include);
  }
}
