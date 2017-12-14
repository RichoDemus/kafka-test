package com.richodemus.test.kafka.json_aggregation

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Materialized
import java.util.Properties
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * to create topic for this run: ./bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic events --replication-factor 1 --partitions 1
 *
 * This will set up a Kafka stream that turns the following two events
 * Name(id:String, name:String)
 * PhoneNumber(id:String, phoneNumber:String)
 *
 * into the aggregated event
 * Result(id:String, name:String, phoneNumber:String)
 */
fun main(args: Array<String>) {
    val topic = "events"

    val config = Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test-${UUID.randomUUID()}")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EventSerde().javaClass)
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    val results = ConcurrentLinkedQueue<Result>()

    val builder = StreamsBuilder()
    /*
    Create a kafka stream with the signature
    Stream<Event>
     */
    val eventStream = builder.stream<String, Event>(topic)
    eventStream
            /*
            group messages by key (which is user id in our case)
            this will turn the stream into a KGroupedStream
            which is basically a stream of key-multivalue pairs like
            Stream<UserId, List<Events>>
             */
            .groupByKey()
            /*
            Merge all events for a user by storing them in an container dto class thing
            this turns the KGroupedStream into a KTable, which is basically a key-value store like
            Map<UserId, Aggregate>
             */
            .aggregate({ Aggregate() }, { _, value, aggregate ->
                aggregate.copy(value)
            }, Materialized.with(Serdes.String(), AggregateSerde()))
            /*
            turns the KTable into a normal Kstream again like
            Stream<Aggregate>
             */
            .toStream()
            /*
            Remove all Aggregates that are incomplete
             */
            .filter { _, value -> value.complete() }
            /*
            Turn the complete aggregate into a result
             */
            .mapValues { Result(it) }
            /*
            Normally we probably want to output the new events to a topic,
            but let just save them in memory
             */
            .foreach { _, value ->
                results.add(value)
            }

    val streams = KafkaStreams(builder.build(), config)
    streams.start()

    while (streams.state() != KafkaStreams.State.RUNNING) {
        Thread.sleep(100L)
    }

    produceSomeEvents(topic)

    println("waiting...")
    var printedResults = 0
    while (printedResults < 3) {
        results.poll()?.let {
            println("Result: $it")
            printedResults++
        }
        Thread.sleep(10L)
    }

    println("Done...")
    streams.close()
}

private fun produceSomeEvents(topic: String) {
    SerdeProducer(topic, EventSerde()).use { producer ->
        val richo = "richo"
        val ohcir = "ohcir"
        val sven = "sven"
        producer.send("incomplete", Name("incomplete", "incomplete"))
        producer.send(sven, PhoneNumber(sven, "00000"))
        producer.send(richo, Name(richo, "Richard"))
        producer.send(sven, Name(sven, "Sven"))
        producer.send(richo, PhoneNumber(richo, "1234"))
        producer.send(ohcir, Name(ohcir, "Drachir"))
        producer.send(ohcir, PhoneNumber(ohcir, "4312"))
    }
}
