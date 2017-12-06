package com.richodemus.test.kafka.compaction

import com.richodemus.test.kafka.StringProducer
import java.util.concurrent.ThreadLocalRandom

/*
./kafka-topics.sh --zookeeper localhost:2181 --create --topic compacted \
--replication-factor=1 --partitions 1 --config cleanup.policy=compact \
--config min.compaction.lag.ms=0 --config delete.retention.ms=0 --config segment.ms=1000
*/
fun main(args: Array<String>) {
    val producer = StringProducer("compacted")
    val topics = listOf("key1", "key2", "key3")
    IntRange(0, 1_000).forEach {
        producer.send(topics.takeRandom(), it.toString())
        Thread.sleep(100L)
    }
    producer.send("key1", "1337")
    producer.send("key2", "1337")
    producer.send("key3", "1337")

    println("Done...")
    Thread.sleep(1000L)
    producer.close()

    val toMap = listOf("", "s").map { it.toUpperCase() }.map { Pair(it, it + "123") }.toMap()
}

private fun <E> List<E>.takeRandom(): E {
    return this[ThreadLocalRandom.current().nextInt(size)]
}
