package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.Properties
import java.util.UUID

/**
 * Helper class to produce messages top a topic
 */
internal class StringProducer(private val topic: String) : Closeable {
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

    fun send(message: String?) = send(UUID.randomUUID().toString(), message)

    fun send(key: String, message: String?): RecordMetadata? {
        val record: ProducerRecord<String, String?> = ProducerRecord(topic, key, message)

        return producer.send(record).get()
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}