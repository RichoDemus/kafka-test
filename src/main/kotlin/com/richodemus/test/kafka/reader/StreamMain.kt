package com.richodemus.test.kafka.reader

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.richodemus.test.kafka.reader.OldEventTypes.ADD_FEED_TO_LABEL
import com.richodemus.test.kafka.reader.OldEventTypes.CHANGE_PASSWORD
import com.richodemus.test.kafka.reader.OldEventTypes.CREATE_LABEL
import com.richodemus.test.kafka.reader.OldEventTypes.CREATE_USER
import com.richodemus.test.kafka.reader.OldEventTypes.USER_SUBSCRIBED_TO_FEED
import com.richodemus.test.kafka.reader.OldEventTypes.USER_UNWATCHED_ITEM
import com.richodemus.test.kafka.reader.OldEventTypes.USER_WATCHED_ITEM
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Properties
import java.util.UUID

/**
 * ./kafka-topics.sh --zookeeper localhost:2181 --create --topic temp --replication-factor 1 --partitions 1
 */
fun main(args: Array<String>) {
    val config = Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-test-${UUID.randomUUID()}")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().javaClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().javaClass)


    val builder = StreamsBuilder()
    val events = builder.stream<String, String>("events")


    events.mapValues { transform(it) }.to("temp")

    val streams = KafkaStreams(builder.build(), config)
    streams.start()
}

private val mapper = jacksonObjectMapper()


private data class EventDTO(val id: String,
                            val type: OldEventTypes,
                            val page: Long,
                            val data: String)

// old events
private data class CreateUserEvent(val eventId: String, val type: OldEventTypes, val userId: String, val username: String, val password: String)
private data class CreateLabelEvent(val eventId: String, val type: OldEventTypes, val userId: String, val labelId: String, val labelName: String)
private data class AddFeedToLabel(val eventId: String, val type: OldEventTypes, val labelId: String, val feedId: String)
private data class UserSubscribedToFeed(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String)
private data class UserWatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)
private data class UserUnwatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)

// new events
private data class NewUserCreatedEvent(val id: String,
                                       val timestamp: String,
                                       val type: NewEventTypes,
                                       val userId: String,
                                       val username: String,
                                       val password: String)

private data class NewLabelCreatedEvent(val id: String,
                                        val timestamp: String,
                                        val type: NewEventTypes,
                                        val userId: String,
                                        val labelId: String,
                                        val labelName: String)

private data class NewFeedAddedToLabelEvent(val id: String,
                                            val timestamp: String,
                                            val type: NewEventTypes,
                                            val labelId: String,
                                            val feedId: String)

private data class NewUserSubscribedToFeed(val id: String,
                                           val timestamp: String,
                                           val type: NewEventTypes,
                                           val userId: String,
                                           val feedId: String)

private data class NewUserWatchedItemEvent(val id: String,
                                           val timestamp: String,
                                           val type: NewEventTypes,
                                           val userId: String,
                                           val feedId: String,
                                           val itemId: String)

private data class NewUserUnwatchedItemEvent(val id: String,
                                             val timestamp: String,
                                             val type: NewEventTypes,
                                             val userId: String,
                                             val feedId: String,
                                             val itemId: String)

private enum class OldEventTypes {
    CREATE_USER,
    CHANGE_PASSWORD,
    CREATE_LABEL,
    ADD_FEED_TO_LABEL,
    USER_SUBSCRIBED_TO_FEED,
    USER_WATCHED_ITEM,
    USER_UNWATCHED_ITEM
}

private enum class NewEventTypes {
    USER_CREATED,
    PASSWORD_CHANGED,
    LABEL_CREATED,
    FEED_ADDED_TO_LABEL,
    USER_SUBSCRIBED_TO_FEED,
    USER_WATCHED_ITEM,
    USER_UNWATCHED_ITEM
}

internal fun transform(msg: String): String {
    val wrapperEvent = mapper.readValue(msg, EventDTO::class.java)
    return when (wrapperEvent.type) {
        CREATE_USER -> convertToNewUserCreatedEvent(wrapperEvent)
        CHANGE_PASSWORD -> msg // no such event yet
        CREATE_LABEL -> convertToNewCreateLabelEvent(wrapperEvent)
        ADD_FEED_TO_LABEL -> convertFeedAddedToLabel(wrapperEvent)
        USER_SUBSCRIBED_TO_FEED -> convertUserSubscripedToFeed(wrapperEvent)
        USER_WATCHED_ITEM -> convertToNewUserWatchedItemEvent(wrapperEvent)
        USER_UNWATCHED_ITEM -> convertUserUnwatchedItem(wrapperEvent)
    }
}

private fun convertToNewUserCreatedEvent(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, CreateUserEvent::class.java)
    val newEvent = NewUserCreatedEvent(wrapperEvent.id,
            now(),
            NewEventTypes.USER_CREATED,
            oldEvent.userId,
            oldEvent.username,
            oldEvent.password)
    return mapper.writeValueAsString(newEvent)
}

private fun convertToNewCreateLabelEvent(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, CreateLabelEvent::class.java)
    val newEvent = NewLabelCreatedEvent(wrapperEvent.id,
            now(),
            NewEventTypes.LABEL_CREATED,
            oldEvent.userId,
            oldEvent.labelId,
            oldEvent.labelName)
    return mapper.writeValueAsString(newEvent)
}

private fun convertFeedAddedToLabel(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, AddFeedToLabel::class.java)
    val newEvent = NewFeedAddedToLabelEvent(oldEvent.eventId,
            now(),
            NewEventTypes.FEED_ADDED_TO_LABEL,
            oldEvent.labelId,
            oldEvent.feedId)
    return mapper.writeValueAsString(newEvent)
}

private fun convertUserSubscripedToFeed(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, UserSubscribedToFeed::class.java)
    val newEvent = NewUserSubscribedToFeed(oldEvent.eventId,
            now(),
            NewEventTypes.USER_SUBSCRIBED_TO_FEED,
            oldEvent.userId,
            oldEvent.feedId)
    return mapper.writeValueAsString(newEvent)
}

private fun convertToNewUserWatchedItemEvent(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, UserWatchedItemEvent::class.java)
    val newEvent = NewUserWatchedItemEvent(oldEvent.eventId,
            now(),
            NewEventTypes.USER_WATCHED_ITEM,
            oldEvent.userId,
            oldEvent.feedId,
            oldEvent.itemId)
    return mapper.writeValueAsString(newEvent)
}

private fun convertUserUnwatchedItem(wrapperEvent: EventDTO): String {
    val oldEvent = mapper.readValue(wrapperEvent.data, UserUnwatchedItemEvent::class.java)
    val newEvent = NewUserUnwatchedItemEvent(oldEvent.eventId,
            now(),
            NewEventTypes.USER_WATCHED_ITEM,
            oldEvent.userId,
            oldEvent.feedId,
            oldEvent.itemId)
    return mapper.writeValueAsString(newEvent)
}

private fun now() = ZonedDateTime.now(ZoneOffset.UTC).toString()
