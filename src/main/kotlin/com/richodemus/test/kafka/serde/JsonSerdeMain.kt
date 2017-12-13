package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.NewFeedAddedToLabelEvent
import com.richodemus.test.kafka.reader.NewLabelCreatedEvent
import com.richodemus.test.kafka.reader.NewUserCreatedEvent
import com.richodemus.test.kafka.reader.NewUserSubscribedToFeed
import com.richodemus.test.kafka.reader.NewUserUnwatchedItemEvent
import com.richodemus.test.kafka.reader.NewUserWatchedItemEvent
import java.time.ZonedDateTime
import java.util.UUID.randomUUID


fun main(args: Array<String>) {
    val topic = "test"
    val consumer = SerdeConsumer(topic) {
        when (it) {
            is NewUserCreatedEvent -> println("User Created: $it")
            is NewLabelCreatedEvent -> println("Label Created: $it")
            is NewFeedAddedToLabelEvent -> println("Feed Added To Label: $it")
            is NewUserSubscribedToFeed -> println("User Subscribed to Feed: $it")
            is NewUserWatchedItemEvent -> println("User Watch Item: $it")
            is NewUserUnwatchedItemEvent -> println("User Unwatched Item: $it")
            else -> println("Unknown messagetype ${it.javaClass}")
        }
    }

    val producer = SerdeProducer(topic)
    val input = listOf(
            NewUserCreatedEvent("id", ZonedDateTime.now(), "userId", "username", "password"),
            NewLabelCreatedEvent("id2", ZonedDateTime.now(), "userId", "labelId", "labelName"),
            NewFeedAddedToLabelEvent("id3", ZonedDateTime.now(), "labelId", "feedId"),
            NewUserSubscribedToFeed("id4", ZonedDateTime.now(), "userId", "feedId"),
            NewUserWatchedItemEvent("id5", ZonedDateTime.now(), "userId", "feedId", "itemId"),
            NewUserUnwatchedItemEvent("id6", ZonedDateTime.now(), "userId", "feedId", "itemId")
    )
    Thread.sleep(2000L)
    input.forEach { producer.send(randomUUID().toString(), it) }

    System.`in`.read()
    producer.close()
    consumer.stop()
}