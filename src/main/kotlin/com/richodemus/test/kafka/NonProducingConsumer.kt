package com.richodemus.test.kafka

import org.slf4j.LoggerFactory

internal class NonProducingConsumer(sourceTopic: String, private val messagesToConsume: Int) {
    private val logger = LoggerFactory.getLogger(javaClass.name)

    private val consumer: Consumer

    init {
        var messages = 0
        logger.info("Time to consume!")
        consumer = Consumer(sourceTopic) { message ->
            logger.info("Consumed: {}", message)
            messages++
            if (messages >= messagesToConsume) {
                stop()
            }
        }
    }

    private fun stop() {
        consumer.stop()
    }
}