package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.Event
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties
import kotlin.concurrent.thread

/**
 * Helper class to consume messages from a topic
 */
internal class SerdeConsumer(topic: String, private val messageListener: (Event) -> Unit) {
    private val logger = LoggerFactory.getLogger(javaClass.name)
    private val consumer: KafkaConsumer<String, Event>
    private var running = true

    init {
        val props = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonEventSerde().deserializer().javaClass)
        //props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props.put(ConsumerConfig.GROUP_ID_CONFIG, topic)
        consumer = KafkaConsumer(props)

        consumer.subscribe(listOf(topic))

        thread {
            while (running) {
                val records = consumer.poll(5000)

                if (records.isEmpty) {
                    logger.info("No messages...")
                    continue
                }

                records.map { it.value() }
                        .forEach { messageListener.invoke(it) }
            }
        }
    }

    fun stop() {
        running = false
    }
}