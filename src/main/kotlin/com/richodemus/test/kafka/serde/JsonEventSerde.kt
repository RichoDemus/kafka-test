package com.richodemus.test.kafka.serde

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.richodemus.test.kafka.reader.Event
import com.richodemus.test.kafka.reader.NewEventTypes
import com.richodemus.test.kafka.reader.NewEventTypes.FEED_ADDED_TO_LABEL
import com.richodemus.test.kafka.reader.NewEventTypes.LABEL_CREATED
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
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

private val mapper = jacksonObjectMapper()

class JsonEventSerde : Serde<Event> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serializer(): Serializer<Event> {
        return JsonEventSerializer()
    }

    override fun deserializer(): Deserializer<Event> {
        return JsonEventDeserializer()
    }

    override fun close() {
    }
}

class JsonEventSerializer : Serializer<Event> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serialize(topic: String?, data: Event?): ByteArray {
        return mapper.writeValueAsBytes(data)
    }

    override fun close() {
    }
}

class JsonEventDeserializer : Deserializer<Event> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun deserialize(topic: String?, data: ByteArray): Event {
        val str = String(data)
        return when {
            str isType USER_CREATED -> mapper.readValue<NewUserCreatedEvent>(data)
        //str isType PASSWORD_CHANGED -> mapper.readValue<>(data)
            str isType LABEL_CREATED -> mapper.readValue<NewLabelCreatedEvent>(data)
            str isType FEED_ADDED_TO_LABEL -> mapper.readValue<NewFeedAddedToLabelEvent>(data)
            str isType USER_SUBSCRIBED_TO_FEED -> mapper.readValue<NewUserSubscribedToFeed>(data)
            str isType USER_WATCHED_ITEM -> mapper.readValue<NewUserWatchedItemEvent>(data)
            str isType USER_UNWATCHED_ITEM -> mapper.readValue<NewUserUnwatchedItemEvent>(data)

            else -> throw IllegalStateException()
        }
    }

    override fun close() {
    }

    private infix fun String.isType(type: NewEventTypes) = type.toString() in this
}
