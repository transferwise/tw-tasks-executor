package com.transferwise.tasks.demoapp;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.tasks.helpers.executors.ExecutorsHelper;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.RequestEntity;
import org.springframework.web.client.RestTemplate;

@Slf4j
//@Disabled("Not meant to be automatically run.")
class DemoAppRealTest {

  private final RestTemplate restTemplate = new RestTemplate();

  @Test
  void boundedExecutorWorks() throws Exception {
    AtomicLong idx = new AtomicLong();
    ExecutorsHelper executorsHelper = new ExecutorsHelper();
    ExecutorService executorService = executorsHelper.newBoundedThreadPoolExecutor("test", 10, 100, Duration.ofHours(1));

    for (int i = 0; i < 150; i++) {
      executorService.submit(() -> {
        ExceptionUtils.doUnchecked(() -> Thread.sleep(1000));
        log.info("{}", idx.incrementAndGet());
      });
      if (i > 140) {
        log.info("{}", executorService);
      }
    }

    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.DAYS);
  }

  // 78000 tasks have to be finished < 50s.
  // 780000 tasks - 960s vs 2700s combID vs randomID.

  @Test
  void allWorks() throws Exception {
    int submitThreads = 30;
    int cycles = 100;
    int lhvCnt = 10;
    int trustlyCnt = 10;
    int achCnt = 5;
    int emailsCnt = 5;

    ExecutorService executor = new ThreadPoolExecutor(
        submitThreads,
        submitThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(submitThreads * 10),
        (r, ex) -> ExceptionUtils.doUnchecked(() -> ex.getQueue().offer(r, 1, TimeUnit.DAYS))
    );

    AtomicLong id = new AtomicLong();
    for (int j = 0; j < cycles; j++) {
      for (int i = 0; i < lhvCnt; i++) {
        executor.submit(() -> submitPayout(id.incrementAndGet(), "Hey", "LHV"));
      }
      for (int i = 0; i < trustlyCnt; i++) {
        executor.submit(() -> submitPayout(id.incrementAndGet(), "Hoy", "TRUSTLY"));
      }
      for (int i = 0; i < achCnt; i++) {
        executor.submit(() -> submitPayout(id.incrementAndGet(), "Hoy", "ACH", 1));
      }
      for (int i = 0; i < emailsCnt; i++) {
        executor.submit(() -> sendEmail(id.incrementAndGet()));
      }
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
  }

  /**
   * Test is more suitable for testing any kind of database limits.
   *
   * <p>So far best results have been observed for 1mln tasks: 280s postgres, 450s mariadb.
   *
   * <p>Postgres has much more favourable architecture for tw-task in general.
   */
  @Test
  void dbPerfTest() throws Exception {
    int submitThreads = 30;
    int tasks = 10000;
    int depth = 10000;

    log.info("Expected tasks count is " + tasks * depth + ".");

    ExecutorService executor = new ThreadPoolExecutor(
        submitThreads,
        submitThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(submitThreads * 10),
        (r, ex) -> ExceptionUtils.doUnchecked(() -> ex.getQueue().offer(r, 1, TimeUnit.DAYS))
    );

    for (int i = 0; i < tasks; i++) {
      long finalI = i;
      executor.submit(() -> {
        try {
          exchange(
              finalI, "/v1/dbPerfTest/addTask", String.valueOf(depth));
        } catch (Throwable t) {
          log.error(t.getMessage(), t);
        }
      });
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
  }

  @Test
  void slowTasksWork() throws Exception {
    int submitThreads = 30;
    int cycles = 10000;

    ExecutorService executor = new ThreadPoolExecutor(
        submitThreads,
        submitThreads,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(submitThreads * 10),
        (r, ex) -> ExceptionUtils.doUnchecked(() -> ex.getQueue().offer(r, 1, TimeUnit.DAYS))
    );

    AtomicLong id = new AtomicLong();
    for (int j = 0; j < cycles; j++) {
      executor.submit(() -> submitSlowTask(id.incrementAndGet()));
    }

    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.DAYS);
  }

  private void submitSlowTask(Long poiId) {
    try {
      exchange(poiId, "/v1/slowtasks/submit", Long.toString(poiId));
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  private void submitPayout(Long poiId, String payload, String type) {
    submitPayout(poiId, payload, type, 5);
  }

  private void submitPayout(Long poiId, String payload, String type, Integer priority) {
    try {
      exchange(
          poiId, "/v1/payout/submit", "{\n"
              + "    \"id\": " + poiId + ",\n"
              + "    \"payload\": \"" + payload + "\",\n"
              + "    \"type\": \"" + type + "\",\n"
              + "    \"priority\": \"" + priority + "\"\n"
              + "}");
    } catch (Throwable t) {
      log.error(t.getMessage(), t);
    }
  }

  private void sendEmail(Long emailId) {
    exchange(emailId, "/v1/email/send", "{\n"
        + "    \"id\": " + emailId + "\n"
        + "}");
  }

  private void exchange(Long id, String url, String body) {
    int port = (id % 1 == 0) ? 12222 : 12223;

    HttpHeaders headers = new HttpHeaders();
    headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

    URI uri = ExceptionUtils.doUnchecked(() -> new URI("http://localhost:" + port + url));
    RequestEntity<String> requestEntity = new RequestEntity<>(body, headers, HttpMethod.POST, uri);

    restTemplate.exchange(requestEntity, String.class);
  }
}
