package com.transferwise.tasks.demoapp

import com.transferwise.tasks.helpers.executors.ExecutorsHelper
import com.transferwise.tasks.test.BaseSpec
import groovy.util.logging.Slf4j
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.http.RequestEntity
import org.springframework.web.client.RestTemplate
import spock.lang.Ignore

import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

@Slf4j
@Ignore("Not meant to be automatically run.")
class DemoAppRealSpec extends BaseSpec {
    private RestTemplate restTemplate = new RestTemplate()

    def "bounded executor works"() {
        when:
            AtomicLong idx = new AtomicLong()
            ExecutorsHelper executorsHelper = new ExecutorsHelper()
            ExecutorService executorService = executorsHelper.newBoundedThreadPoolExecutor("test", 10, 100, Duration.ofHours(1))

            for (int i = 0; i < 150; i++) {
                executorService.submit {
                    Thread.sleep(1000)
                    println idx.incrementAndGet()
                }
                if (i > 140) {
                    println executorService
                }
            }

            executorService.shutdown()
            executorService.awaitTermination(1, TimeUnit.DAYS)
        then:
            1 == 1
    }

    def "all works"() {
        given:
            int SUBMIT_THREADS = 30
            int CYCLES = 100
            int LHV_CNT = 10
            int TRUSTLY_CNT = 10
            int ACH_CNT = 5
            int EMAILS_CNT = 5

            ExecutorService executor = new ThreadPoolExecutor(SUBMIT_THREADS, SUBMIT_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(SUBMIT_THREADS * 10), new RejectedExecutionHandler() {
                @Override
                void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    executor.getQueue().offer(r, 1, TimeUnit.DAYS)
                }
            })

        when:
            AtomicLong id = new AtomicLong()
            for (int j = 0; j < CYCLES; j++) {
                for (int i = 0; i < LHV_CNT; i++) {
                    executor.submit {
                        submitPayout(id.incrementAndGet(), "Hey", "LHV")
                    }
                }
                for (int i = 0; i < TRUSTLY_CNT; i++) {
                    executor.submit {
                        submitPayout(id.incrementAndGet(), "Hoy", "TRUSTLY")
                    }
                }
                for (int i = 0; i < ACH_CNT; i++) {
                    executor.submit {
                        submitPayout(id.incrementAndGet(), "Hoy", "ACH", 1)
                    }
                }
                for (int i = 0; i < EMAILS_CNT; i++) {
                    executor.submit {
                        sendEmail(id.incrementAndGet())
                    }
                }
            }

            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.DAYS)
        then:
            1 == 1
    }

    def "slow tasks work"() {
        given:
            int SUBMIT_THREADS = 30
            int CYCLES = 10000

            ExecutorService executor = new ThreadPoolExecutor(SUBMIT_THREADS, SUBMIT_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(SUBMIT_THREADS * 10), new RejectedExecutionHandler() {
                @Override
                void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    executor.getQueue().offer(r, 1, TimeUnit.DAYS)
                }
            })

        when:
            AtomicLong id = new AtomicLong()
            for (int j = 0; j < CYCLES; j++) {
                executor.submit {
                    submitSlowTask(id.incrementAndGet())
                }
            }

            executor.shutdown()
            executor.awaitTermination(1, TimeUnit.DAYS)
        then:
            1 == 1
    }

    private void submitSlowTask(Long poiId) {
        try {
            exchange(poiId, "/v1/slowtasks/submit", "${poiId}")
        }
        catch (Throwable t) {
            log.error(t.getMessage(), t)
        }
    }

    private void submitPayout(Long poiId, String payload, String type, Integer priority = 5) {
        try {
            exchange(poiId, "/v1/payout/submit", """{
    "id": ${poiId},
    "payload": "${payload}",
    "type": "${type}",
    "priority": "${priority}"
}""")
        }
        catch (Throwable t) {
            log.error(t.getMessage(), t)
        }
    }

    private void sendEmail(Long emailId) {
        exchange(emailId, "/v1/email/send", """{
    "id": ${emailId}
}""")

    }

    private void exchange(Long id, String url, String body) {
        int port = (id % 2 == 0) ? 12222 : 12223

        HttpHeaders headers = new HttpHeaders()
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE)

        RequestEntity<String> requestEntity = new RequestEntity<>(body, headers, HttpMethod.POST, new URI("http://localhost:${port}" + url))

        restTemplate.exchange(requestEntity, String)
    }

}
