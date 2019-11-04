package com.transferwise.tasks.handler

import com.transferwise.common.baseutils.clock.ClockHolder
import com.transferwise.tasks.domain.ITask
import com.transferwise.tasks.domain.Task
import spock.lang.Specification

import java.time.ZonedDateTime

class FixSecondsRetryPolicyTest extends Specification {

    def "zero retries"() {
        given:
            def policy = new FixSecondsRetryPolicy()
            ITask task = new Task(processingTriesCount: 0)
        when:
            def retime = policy.getRetryTime(task, null)
        then:
            retime == null
    }

    def "first retry, second retry, nth retry"() {
        given:
            def policy = Spy(new FixSecondsRetryPolicy(1, 2))
            def now = ZonedDateTime.now(ClockHolder.getClock())
            policy.now() >> now
        when:
            ITask task = new Task(processingTriesCount: 0)
            def retime = policy.getRetryTime(task, null)
        then:
            retime == now.plusSeconds(1)
        when:
            task = new Task(processingTriesCount: 1)
            retime = policy.getRetryTime(task, null)
        then:
            retime == now.plusSeconds(3)
        when:
            task = new Task(processingTriesCount: 3)
            retime = policy.getRetryTime(task, null)
        then:
            retime == null
    }
}
