package com.richodemus.test.kafka.json_aggregation

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.StringSerializer
import java.io.Closeable
import java.util.Properties

/**
 * Helper class to produce messages top a topic
 */
internal class SerdeProducer<T>(private val topic: String, serde: Serde<T>) : Closeable {
    private val producer: KafkaProducer<String, T>

    init {
        val props = Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serde.serializer().javaClass)

        producer = KafkaProducer(props)
    }

    fun send(key: String, message: T): RecordMetadata? {
        val record: ProducerRecord<String, T> = ProducerRecord(topic, key, message)

        return producer.send(record).get()
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}
