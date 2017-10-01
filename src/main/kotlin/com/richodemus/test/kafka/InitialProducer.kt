package com.richodemus.test.kafka

import org.slf4j.LoggerFactory
import java.util.UUID

internal class InitialProducer(topic: String, private val messagesToCreate: Int) : Runnable {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val producer: Producer = Producer(topic)

    override fun run() {
        logger.info("Time to produce!")
        IntRange(1, messagesToCreate).forEach {
            val id = UUID.randomUUID().toString()

            val recordMetadata = producer.send(id, Message(id, 1))
        }
        producer.close()
    }
}
