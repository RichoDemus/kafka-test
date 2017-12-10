package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.NewEventTypes.FEED_ADDED_TO_LABEL
import com.richodemus.test.kafka.reader.NewEventTypes.LABEL_CREATED
import com.richodemus.test.kafka.reader.NewEventTypes.PASSWORD_CHANGED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_CREATED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_SUBSCRIBED_TO_FEED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_UNWATCHED_ITEM
import com.richodemus.test.kafka.reader.NewEventTypes.USER_WATCHED_ITEM
import com.richodemus.test.kafka.reader.NewFeedAddedToLabelEvent
import com.richodemus.test.kafka.reader.NewLabelCreatedEvent
import com.richodemus.test.kafka.reader.NewUserCreatedEvent
import com.richodemus.test.kafka.reader.NewUserSubscribedToFeed
import com.richodemus.test.kafka.reader.NewUserUnwatchedItemEvent
import com.richodemus.test.kafka.reader.NewUserWatchedItemEvent
import java.time.ZonedDateTime
import java.util.UUID.randomUUID


fun main(args: Array<String>) {
    val topic = "asdf"
    val consumer = SerdeConsumer(topic) {
        when (it.type()) {
            USER_CREATED -> println("User Created: ${it as NewUserCreatedEvent}")
            PASSWORD_CHANGED -> TODO()
            LABEL_CREATED -> println("Label Created: ${it as NewLabelCreatedEvent}")
            FEED_ADDED_TO_LABEL -> println("Feed Added To Label: ${it as NewFeedAddedToLabelEvent}")
            USER_SUBSCRIBED_TO_FEED -> println("User Subscribed to Feed: ${it as NewUserSubscribedToFeed}")
            USER_WATCHED_ITEM -> println("User Watch Item: ${it as NewUserWatchedItemEvent}")
            USER_UNWATCHED_ITEM -> println("User Unwatched Item: ${it as NewUserUnwatchedItemEvent}")
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
    input.forEach { producer.send(randomUUID().toString(), it) }

    System.`in`.read()
    producer.close()
    consumer.stop()
}