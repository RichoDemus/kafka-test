package com.richodemus.test.kafka

import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Produces messages to a defined topic
 */
internal class InitialProducer(topic: String, private val messagesToCreate: Int) : Runnable {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val producer: Producer = Producer(topic)

    override fun run() {
        logger.info("Time to produce!")
        if(messagesToCreate < 1) {
            return
        }
        IntRange(1, messagesToCreate).forEach {
            val id = UUID.randomUUID().toString()

            val recordMetadata = producer.send(id, Message(id, listOf("Producer")))
        }
        producer.close()
    }
}
