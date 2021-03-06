package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

/**
 * Helper class to produce messages top a topic
 */
internal class Producer(private val topic: String) {
    private val mapper = jacksonObjectMapper()
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "35.189.72.224:9092")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        producer = KafkaProducer(props)
    }

    fun send(key: String, message: Message): RecordMetadata? {
        val json = mapper.writeValueAsString(message)
        val record: ProducerRecord<String, String> = ProducerRecord(topic, key, json)

        return producer.send(record).get()
    }

    fun close() {
        producer.flush()
        producer.close()
    }
}
