package com.transferwise.tasks.utils;

import lombok.experimental.UtilityClass;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

@UtilityClass
public class ListUtils {

  public static  <T, R> List<R> chunkIntoVariableSizeAndOperate(
      List<T> originalList,
      int[] variableChunkSizeBucket,
      BiFunction<List<T>, Integer, List<R>> operation) {

    Objects.requireNonNull(originalList);
    Objects.requireNonNull(variableChunkSizeBucket);
    Objects.requireNonNull(operation);

    List<R> result = new ArrayList<>();
    int idx = 0;

    while (true) {
      int idsLeft = originalList.size() - idx;
      if (idsLeft < 1) {
        return result;
      }
      int bucketId = 0;
      for (int j = variableChunkSizeBucket.length - 1; j >= 0; j--) {
        if (variableChunkSizeBucket[j] <= idsLeft) {
          bucketId = j;
          break;
        }
      }
      int questionsCount = variableChunkSizeBucket[bucketId];
      List<R> intermediateResult = operation.apply(originalList.subList(idx, idx + questionsCount), bucketId);
      result.addAll(intermediateResult);

      idx += questionsCount;
    }
  }
}
