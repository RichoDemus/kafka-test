package com.richodemus.test.kafka.aggregation

import com.richodemus.test.kafka.StringProducer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.TimeWindows
import java.util.Properties
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.LongAdder
import kotlin.concurrent.thread

/**
 * ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic events --replication-factor 1 --partitions 1
 */
fun main(args: Array<String>) {
    val topic = "events"
    val config = Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test-${UUID.randomUUID()}")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val totalNumberOfMessages = LongAdder()
    val gameroundMessages = LongAdder()
    val mergedMessages = LongAdder()


    val builder = StreamsBuilder()
    val events = builder.stream<String, String>(topic)

    events
            .peek { _, _ -> totalNumberOfMessages.increment() }
            .groupBy { _, value -> value.split(",")[0] }
            .windowedBy(TimeWindows.of(TimeUnit.DAYS.toMillis(1)))
            .aggregate({ "" }, { _, value, aggregate ->
                aggregate + value.split(",")[1]
            })
            .toStream()
            .peek { _, _ -> mergedMessages.increment() }
//            .filter { _, value -> value.length > 2 }
            .foreach { key, value -> println("\nResult: ${key.key()}: $value") } // in reality we want to write it to a new topic, use .to to do that

    thread(isDaemon = true) {
        while (true) {
            if (mergedMessages.toLong() > 0)
                println("$totalNumberOfMessages messages, $gameroundMessages gameround messages, $mergedMessages merged, ${gameroundMessages.toDouble() / mergedMessages.toDouble()} ratio")
            else
                print(".")
            Thread.sleep(1000L)
        }
    }

    val streams = KafkaStreams(builder.build(), config)
    streams.start()
    Runtime.getRuntime().addShutdownHook(Thread(streams::close))

    Thread.sleep(2000L)
    val firstId = UUID.randomUUID()
    StringProducer(topic).use { producer ->
        val secondId = UUID.randomUUID()
        val thirdId = UUID.randomUUID()
        producer.send("$secondId,h")
        producer.send("$firstId,h")
        producer.send("$firstId,e")
        producer.send("$firstId,l")
        producer.send("$thirdId,h")
        producer.send("$firstId,l")
        producer.send("$firstId,o")
        producer.send("$thirdId,o")
        producer.send("$secondId,i")
    }

    System.`in`.read()
    StringProducer(topic).use { producer ->
        producer.send("$firstId,!")
    }
    println("pong")
    System.`in`.read()
    streams.close()
}