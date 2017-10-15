package com.richodemus.test.kafka

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import java.util.Properties

/**
 * Helper class to set up a stream
 */
class Stream(sourceTopic: String, destinationTopic: String) {
    private val kafkaStreams: KafkaStreams
    init {
        val builder = KStreamBuilder()
        val config = Properties()
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application2")
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        config.put(ConsumerConfig.GROUP_ID_CONFIG, sourceTopic)

        val stream = builder.stream<String, String>(sourceTopic)

        val mapper = jacksonObjectMapper()
        stream.map { key, value ->
            val message = mapper.readValue<Message>(value)
            val newMessage = message.copy(tags = message.tags.plus("$sourceTopic Stream $destinationTopic"))
            KeyValue(key, mapper.writeValueAsString(newMessage))
        }.to(destinationTopic)

        kafkaStreams = KafkaStreams(builder, config)
        kafkaStreams.start()
    }

    fun close() {
        kafkaStreams.close()
    }
}
