package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.UUID

internal class AdditionConsumer(private val topic: String, private val messagesToConsume: Int) : Runnable {
    private val mapper = jacksonObjectMapper()
    private val logger = LoggerFactory.getLogger(javaClass.name)

    private val consumer: KafkaConsumer<String, String>
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties()
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KafkaExampleConsumer")
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString())
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        consumer = KafkaConsumer(props)

        val props2 = Properties()
        props2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props2.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        producer = KafkaProducer(props2)
    }

    override fun run() {
        // Subscribe to the topic.
        consumer.subscribe(listOf(topic))

        var numberOfDoneMessages = 0

        while (numberOfDoneMessages < messagesToConsume) {
            val records = consumer.poll(5000)

            if (records.isEmpty) {
                logger.info("No messages...")
                continue
            }

            records.forEach { record ->
                val message = mapper.readValue<Message>(record.value())
                logger.info("Received: {}: {}", record.key(), message)

                val newMessage = message.copy(counter = message.counter + 1)
                val id = UUID.randomUUID().toString()

                val json = mapper.writeValueAsString(newMessage)

                val newRecord: ProducerRecord<String, String> = ProducerRecord(topic, id, json)

                if (newMessage.counter <= 100) {
                    val recordMetadata = producer.send(newRecord).get()
                } else {
                    numberOfDoneMessages++
                }
            }
        }
    }
}
