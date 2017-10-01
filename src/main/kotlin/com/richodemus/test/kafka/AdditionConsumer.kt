package com.richodemus.test.kafka

import org.slf4j.LoggerFactory
import java.util.UUID

internal class AdditionConsumer(private val topic: String, private val messagesToConsume: Int) {
    private val logger = LoggerFactory.getLogger(javaClass.name)

    private val consumer: Consumer
    private val producer = Producer()

    init {
        var numberOfDoneMessages = 0
        logger.info("Time to consume!")
        consumer = Consumer { message ->
            val newMessage = message.copy(counter = message.counter + 1)
            val id = UUID.randomUUID().toString()

            if (newMessage.counter <= 100) {
                val recordMetadata = producer.send(id, newMessage)
            } else {
                numberOfDoneMessages++
                if (numberOfDoneMessages >= messagesToConsume) {
                    stop()
                }
            }
        }
    }

    private fun stop() {
        consumer.stop()
        producer.close()
    }
}
