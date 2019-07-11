package com.transferwise.tasks.testappa

import com.transferwise.tasks.helpers.kafka.messagetotask.CoreKafkaListener
import com.transferwise.tasks.helpers.kafka.messagetotask.IKafkaMessageHandler
import com.transferwise.tasks.helpers.kafka.messagetotask.KafkaMessageHandlerRegistry
import com.transferwise.tasks.impl.tokafka.test.IToKafkaTestHelper
import com.transferwise.tasks.test.BaseIntSpec
import io.micrometer.core.instrument.MeterRegistry
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.kafka.KafkaProperties
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Primary
import org.springframework.test.context.TestPropertySource
import spock.util.concurrent.PollingConditions

import java.util.concurrent.atomic.AtomicInteger

import static org.awaitility.Awaitility.await

@TestPropertySource(properties = [
		"spring.main.allow-bean-definition-overriding=true"
])
class CoreKafkaListenerIntSpec extends BaseIntSpec {

	@Autowired
	private IToKafkaTestHelper toKafkaTestHelper

	@Autowired
	private KafkaProperties kafkaProperties

	@Autowired
	private MeterRegistry meterRegistry

	@Autowired
	private CoreKafkaListener kafkaListener

	private final static TOPIC_NAME = "myTopic"

	def "should track the number of messages consumed per topic"() {
		given:
			Thread thread
			int N = 10
			Map<String, AtomicInteger> messagesReceivedCounts = [:]

			for (int i = 0; i < N; i++) {
				String message = "Message" + i
				messagesReceivedCounts[message] = new AtomicInteger(0)
				toKafkaTestHelper.sendDirectKafkaMessage(TOPIC_NAME, message)
			}
		when:
			kafkaListener.kafkaDataCenterPrefixes = []
			thread = new Thread({ kafkaListener.poll([TOPIC_NAME]) as Runnable})
			thread.start()
		then:
			new PollingConditions(timeout: 10, delay: 0.5).eventually {
				meterRegistry.find("twTasks.coreKafka.processedMessagesCount")
						.tag("topic", TOPIC_NAME).counter().count() == N
			}
		cleanup:
			thread.interrupt()
	}

	@TestConfiguration
	static class ContextConfiguration {
		@Bean
		KafkaMessageHandlerRegistry twTasksKafkaMessageHandlerRegistry() {
			return new KafkaMessageHandlerRegistry()
		}

		@Bean
		IKafkaMessageHandler aHandlerForTopicA() {
			return new IKafkaMessageHandler() {

				@Override
				List<IKafkaMessageHandler.Topic> getTopics() {
					Collections.singletonList(new IKafkaMessageHandler.Topic().setAddress(TOPIC_NAME))
				}

				@Override
				boolean handles(String topic) {
					return topic == TOPIC_NAME
				}

				@Override
				void handle(ConsumerRecord record) {
				}
			}
		}
	}
}
