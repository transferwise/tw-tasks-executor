package com.transferwise.tasks.utils;

import com.transferwise.tasks.dao.ITaskDao;
import com.transferwise.tasks.domain.BaseTask;

/**
 * Only allows conversion from wider type to narrower type, as there should be no data loss.
 */
public final class DomainUtils {

  private DomainUtils() {
    throw new AssertionError();
  }

  @SuppressWarnings("unchecked")
  public static <T> T convert(ITaskDao.StuckTask task, Class<T> resultClass) {
    if (task == null) {
      return null;
    }

    if (resultClass.equals(BaseTask.class)) {
      return (T) new BaseTask()
          .setId(task.getVersionId().getId())
          .setPriority(task.getPriority())
          .setType(task.getType())
          .setVersion(task.getVersionId().getVersion());
    }
    throw new IllegalArgumentException("No conversion is supprorted from " + resultClass.getCanonicalName() + ".");
  }
}
