package com.transferwise.tasks.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;


class ListUtilsTest {

  @Test
  public void chunkIntoVariableSizeAndOperate() {
    List<Integer> originalList = IntStream.range(0, 61).boxed().collect(Collectors.toList());
    int[] bucket = { 1, 10, 50 };

    AtomicInteger counter = new AtomicInteger(0);
    BiFunction<List<Integer>, Integer, List<Integer>> operation = (chunkedList, bucketId) -> {
      assertEquals(chunkedList.size(), bucket[bucket.length - counter.incrementAndGet()]);
      return chunkedList; //no operation is performed, just return the chunked list
    };

    List<Integer> result = ListUtils.chunkIntoVariableSizeAndOperate(originalList, bucket, operation);
    assertEquals(result.size(), originalList.size());
  }
}