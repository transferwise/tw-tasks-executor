package com.transferwise.tasks.helpers

import com.transferwise.tasks.test.BaseIntSpec
import org.springframework.beans.factory.annotation.Autowired

import static org.awaitility.Awaitility.await

class ErrorLoggingThrottlerIntSpec extends BaseIntSpec {
    @Autowired
    private ErrorLoggingThrottler errorLoggingThrottler

    // Very basic test to check, if the component is correctly initialized and set up.
    def "throttling should work"() {
        when:
        await().until({ errorLoggingThrottler.canLogError() })

        int cnt = 0
        for (int i = 0; i < 10; i++) {
            cnt = errorLoggingThrottler.canLogError() ? 1 : 0
        }
        then:
        cnt == 0;
    }
}
