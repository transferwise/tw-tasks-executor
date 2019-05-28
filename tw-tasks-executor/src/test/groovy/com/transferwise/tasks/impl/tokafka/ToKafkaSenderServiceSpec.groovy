package com.transferwise.tasks.impl.tokafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.transferwise.common.baseutils.tracing.IXRequestIdHolder
import com.transferwise.tasks.ITasksService
import org.apache.commons.lang3.RandomStringUtils
import spock.lang.Specification
import spock.lang.Unroll

class ToKafkaSenderServiceSpec extends Specification {
    private ObjectMapper objectMapper = Mock()
    private ITasksService taskService = Mock()
    private IXRequestIdHolder xRequestIdHolder = Mock()
    private ToKafkaSenderService toKafkaSenderService = new ToKafkaSenderService(objectMapper, taskService, 8, xRequestIdHolder)

    void "payload is being converted"() {
        when:
            toKafkaSenderService.sendMessage(new IToKafkaSenderService.SendMessageRequest().setPayload("abc"))
        then:
            1 * objectMapper.writeValueAsString("abc") >> "abc"
            1 * taskService.addTask(_)
    }

    void "payload is being converted. Batch case"() {
        when:
            toKafkaSenderService.sendMessages(new IToKafkaSenderService.SendMessagesRequest().add(new IToKafkaSenderService.SendMessagesRequest.Message().setPayload("abc")))
        then:
            1 * objectMapper.writeValueAsString("abc") >> "abc"
            1 * taskService.addTask(_)
    }

    void "payloadString is used as it is"() {
        when:
            toKafkaSenderService.sendMessage(new IToKafkaSenderService.SendMessageRequest().setPayloadString("abc"))
        then:
            0 * objectMapper.writeValueAsString(_)
            1 * taskService.addTask(_)
    }

    void "payloadString is being converted. Batch case"() {
        when:
            toKafkaSenderService.sendMessages(new IToKafkaSenderService.SendMessagesRequest().add(new IToKafkaSenderService.SendMessagesRequest.Message().setPayloadString("abc")))
        then:
            0 * objectMapper.writeValueAsString("abc")
            1 * taskService.addTask(_)
    }

    @Unroll
    void "batches are calculated correctly #messageDescriptor produces #expectedBatch"() {
        int MB = 1024 * 1024

        when:
            def messages = messageDescriptor.split(" ").collect {
                def (String id, String size) = it.split(":")
                mockMessage(Integer.parseInt(size - "MB") * MB, id)
            }
            def batchDescriptor = toKafkaSenderService.splitToBatches(messages, 10 * MB).collect { it ->
                "[" + it.collect { it.message }.join(", ") + "]"
            }.join()
        then:
            batchDescriptor == expectedBatch

        where:
            messageDescriptor                     || expectedBatch
            "1:6MB"                               || "[1]"
            "1:6MB 2:6MB"                         || "[1][2]"
            "1:6MB 2:4MB"                         || "[1, 2]"
            "1:6MB 2:6MB 3:12MB"                  || "[1][2][3]"
            "1:3MB 2:3MB 3:3MB"                   || "[1, 2, 3]"
            "1:3MB 2:3MB 3:5MB"                   || "[1, 2][3]"
            "1:11MB"                              || "[1]"
            "1:11MB 2:11MB"                       || "[1][2]"
            "1:4MB 2:1MB 3:2MB 4:1MB 5:4MB 6:2MB" || "[1, 2, 3, 4][5, 6]"
            "1:4MB 2:1MB 3:2MB 4:1MB 5:2MB 6:6MB" || "[1, 2, 3, 4, 5][6]"
    }

    private ToKafkaMessages.Message mockMessage(int size, String id) {
        def mock = Mock(ToKafkaMessages.Message)
        mock.getMessage() >> id
        mock.getApproxSize() >> size
        mock
    }

    void "messages are split to batches"() {
        given: "a huge request"
            toKafkaSenderService
            def millis = System.currentTimeMillis()
            def request = new IToKafkaSenderService.SendMessagesRequest()
            List<String> randomStrings = (1..1024).collect {
                RandomStringUtils.randomAlphabetic(1024)
            } // 1024 strings of 1 KiB
            50.times {
                Collections.shuffle(randomStrings)
                request.add(new IToKafkaSenderService.SendMessagesRequest.Message().setPayloadString(randomStrings.join()))
                // 1 MiB ~1 MB
            }
            println("Created 50 MB of random strings in ${System.currentTimeMillis() - millis} ms.")
        when:
            toKafkaSenderService.sendMessages(request)
        then: 'At least 8 batches have been sent'
            (8.._) * taskService.addTask(_)
    }

    void "messages that are bigger than batch size are still being sent"() {
        given: "a huge request"
            def millis = System.currentTimeMillis()
            def request = new IToKafkaSenderService.SendMessagesRequest()
            def s1 = RandomStringUtils.randomAlphabetic(1024 * 1024 * 6) // two 6 MiB strings
            def s2 = RandomStringUtils.randomAlphabetic(1024 * 1024 * 6) // two 6 MiB strings
            request.add(new IToKafkaSenderService.SendMessagesRequest.Message().setPayloadString(s1))
            request.add(new IToKafkaSenderService.SendMessagesRequest.Message().setPayloadString(s2))

            println("Created 2 random 6 MB strings in ${System.currentTimeMillis() - millis} ms.")
        when:
            toKafkaSenderService.sendMessages(request)
        then:
            2 * taskService.addTask(_)
    }
}
