package com.richodemus.test.kafka

import org.slf4j.LoggerFactory
import java.util.UUID

internal class AdditionConsumer(private val name: String, sourceTopic: String, targetTopic: String) {
    private val logger = LoggerFactory.getLogger(javaClass.name)

    private val consumer: Consumer
    private val producer = Producer(targetTopic)

    init {
        logger.info("Time to consume!")
        consumer = Consumer(sourceTopic) { message ->
            val newMessage = message.copy(tags = message.tags.plus(name))
            val id = UUID.randomUUID().toString()

            val recordMetadata = producer.send(id, newMessage)
        }
    }

    fun stop() {
        consumer.stop()
        producer.close()
    }
}
