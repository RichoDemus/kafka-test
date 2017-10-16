package com.richodemus.test.kafka

import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

/**
 * Simple test of the throughput of the Kafka Producer API
 * My MacBook pushes almost 50k/s
 */
fun main(args: Array<String>) {
    val producer = Producer("dump")
    val numberOfMessages = 1_000_000
    val executor = Executors.newFixedThreadPool(1000)

    val stream = IntRange(1, numberOfMessages)
            .map {
                Message(it.toString(), emptyList())
            }.map {
        Callable {
            producer.send(it.id, it)
        }
    }
    Thread.sleep(2000L)
    println("Starting...")
    val timeTaken = measureTimeMillis {
        stream.map { executor.submit(it) }
                .forEach {
                    it.get()
                }

    }
    println("Done...")
    Thread.sleep(2000)

    producer.close()

    println("sent $numberOfMessages messages in ${timeTaken / 1000} seconds")
    executor.shutdown()
}