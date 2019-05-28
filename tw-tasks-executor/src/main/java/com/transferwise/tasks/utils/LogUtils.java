package com.transferwise.tasks.utils;

import com.transferwise.tasks.domain.ITaskVersionId;
import lombok.experimental.UtilityClass;

@UtilityClass
public class LogUtils {
    public static String asParameter(ITaskVersionId taskVersionId) {
        return "'" + taskVersionId.getId() + "-" + taskVersionId.getVersion() + "'";
    }
}
