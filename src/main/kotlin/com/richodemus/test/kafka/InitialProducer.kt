package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import java.util.UUID

internal class InitialProducer(private val topic: String, private val messagesToCreate: Int) : Runnable {
    private val mapper = jacksonObjectMapper()
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        producer = KafkaProducer(props)
    }

    override fun run() {
        try {
            IntRange(1, messagesToCreate).forEach {
                val id = UUID.randomUUID().toString()

                val json = mapper.writeValueAsString(Message(id, 1))

                val record: ProducerRecord<String, String> = ProducerRecord(topic, id, json)

                val recordMetadata = producer.send(record).get()
            }
        } finally {
            producer.flush()
            producer.close()
        }
    }
}
