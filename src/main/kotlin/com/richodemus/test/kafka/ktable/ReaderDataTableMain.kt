package com.richodemus.test.kafka.ktable

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.QueryableStoreTypes
import java.util.Properties
import java.util.UUID
import java.util.regex.Pattern
import kotlin.concurrent.thread

fun main(args: Array<String>) {
    val topic = "events_v2"

    // Lots of config
    val streamsConfiguration = Properties()
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example-${UUID.randomUUID()}")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-lambda-example-client-${UUID.randomUUID()}")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass.name)
    // Records should be flushed every 10 seconds. This is less than the default
    // in order to keep this example interactive.
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000)
    // For illustrative purposes we disable record caches
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val builder = StreamsBuilder()
    val readerEvents = builder.stream<String, String>(topic)

    //language=REGEXP
    val pattern = "\"id\":\"(.*?)\""
    Pattern.compile(pattern)
    // turn the stream of UUIDs into a key value table
    val table = readerEvents
            .filter { _, value -> value.contains("LABEL_CREATED") }
            .map { key, value ->
                println("peek. Key: $key\tvalue: $value")
                KeyValue(key, value)
            }
            .groupByKey()
            .reduce({ _, right -> right }, Materialized.`as`("materialized"))
//            .groupByKey()
//            .reduce({ _, right -> right }, Materialized.`as`("materialized"))


    val streams = KafkaStreams(builder.build(), streamsConfiguration)
    streams.cleanUp()
    streams.start()


    Runtime.getRuntime().addShutdownHook(Thread(streams::close))


//    // post a UUID to a random key every 100ms
//    thread(isDaemon = true) {
//        val producer = StringProducer(topic)
//        val keys = listOf("one", "two", "three", "four", "five", "sex")
//        while (true) {
//            producer.send(keys.takeRandom(), UUID.randomUUID().toString())
//            Thread.sleep(100L)
//        }
//    }

    // print the key value store every second
    thread(isDaemon = true) {
        val queryableStoreName = table.queryableStoreName()
        while (true) {
            val view = waitUntilStoreIsQueryable(queryableStoreName, QueryableStoreTypes.keyValueStore<String, String>(), streams)
            view.all().forEach {
                println("State: key: ${it.key},\tvalue: ${it.value}")
            }
            println()
            Thread.sleep(1000L)
        }
    }

    System.`in`.read()
    streams.close()
}