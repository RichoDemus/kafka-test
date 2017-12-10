package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.Event
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

/**
 * Helper class to produce messages top a topic
 */
internal class SerdeProducer(private val topic: String) {
    private val producer: KafkaProducer<String, Event>

    init {
        val props = Properties()
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer")
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonEventSerde().serializer().javaClass)

        producer = KafkaProducer(props)
    }

    fun send(key: String, message: Event): RecordMetadata? {
        val record: ProducerRecord<String, Event> = ProducerRecord(topic, key, message)

        return producer.send(record).get()
    }

    fun close() {
        producer.flush()
        producer.close()
    }
}