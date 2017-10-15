package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Helper class to consume messages from a topic
 */
internal class Consumer(topic: String, private val messageListener: (Message) -> Unit) {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val mapper = jacksonObjectMapper()
    private val consumer: KafkaConsumer<String, String>
    private var running = true

    init {
        val props = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topic)
        consumer = KafkaConsumer(props)

        consumer.subscribe(listOf(topic))

        Thread {
            while (running) {
                val records = consumer.poll(5000)

                if (records.isEmpty) {
                    logger.info("No messages...")
                    continue
                }

                records.map {
                    logger.debug("Received: {}: {}", it.key(), it.value())
                    it.value()
                }
                        .map { mapper.readValue<Message>(it) }
                        .forEach { messageListener.invoke(it) }
            }
        }.start()
    }

    fun stop() {
        running = false
    }
}