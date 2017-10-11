package com.richodemus.test.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.KStreamBuilder
import java.util.Properties

fun main(args: Array<String>) {

    val sourceTopic = "source"
    val intemediate = "intermediate"
    val destinationTopic = "destination"


    val stream = SimpleStream(sourceTopic, intemediate)
    val stream2 = SimpleStream(intemediate, destinationTopic)



    Thread.sleep(10000000L)
    Runtime.getRuntime().addShutdownHook(Thread(stream::close))
    Runtime.getRuntime().addShutdownHook(Thread(stream2::close))
}

private class SimpleStream(source: String, destination: String) {
    val kafkaStreams: KafkaStreams
    init {
        val builder = KStreamBuilder()
        val config = Properties()
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-$source")
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

        val stream = builder.stream<String, String>(source)

        stream.peek { key, value ->  println("Shuffling $value from $source to $destination")}.to(destination)

        kafkaStreams = KafkaStreams(builder, config)
        kafkaStreams.start()
    }

    fun close() {
        kafkaStreams.close()
    }
}