package com.richodemus.test.kafka

import org.slf4j.LoggerFactory

/**
 * Simply consumes messages from a topic and stops after it has consumed a defined amount
 */
internal class NonProducingConsumer(sourceTopic: String, private val messagesToConsume: Int) {
    private val logger = LoggerFactory.getLogger(javaClass.name)

    private val consumer: Consumer

    var running = false
        private set

    init {
        var messages = 0
        logger.info("Time to consume!")
        running = true
        consumer = Consumer(sourceTopic) { message ->
            logger.info("Consumed: {}", message)
            messages++
            if (messages >= messagesToConsume) {
                stop()
            }
        }
    }

    private fun stop() {
        running = false
        consumer.stop()
    }
}