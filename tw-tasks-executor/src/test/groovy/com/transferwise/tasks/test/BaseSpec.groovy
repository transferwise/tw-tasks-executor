package com.transferwise.tasks.test

import com.transferwise.common.baseutils.clock.TestClock
import spock.lang.Specification

class BaseSpec extends Specification {
    def cleanup() {
        TestClock.reset()
    }
}
