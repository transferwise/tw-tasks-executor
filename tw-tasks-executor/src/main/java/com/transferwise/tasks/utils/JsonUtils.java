package com.transferwise.tasks.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.transferwise.common.baseutils.ExceptionUtils;
import lombok.experimental.UtilityClass;

@UtilityClass
public class JsonUtils {
    public static String toJson(ObjectMapper objectMapper, Object obj) {
        return ExceptionUtils.doUnchecked(() -> objectMapper.writeValueAsString(obj));
    }

    public static <T> T fromJson(ObjectMapper objectMapper, String st, Class<T> type) {
        return ExceptionUtils.doUnchecked(() -> objectMapper.readValue(st, type));
    }
}
