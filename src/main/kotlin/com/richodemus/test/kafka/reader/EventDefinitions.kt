package com.richodemus.test.kafka.reader

import com.richodemus.test.kafka.reader.NewEventTypes.FEED_ADDED_TO_LABEL
import com.richodemus.test.kafka.reader.NewEventTypes.LABEL_CREATED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_CREATED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_SUBSCRIBED_TO_FEED
import com.richodemus.test.kafka.reader.NewEventTypes.USER_UNWATCHED_ITEM
import com.richodemus.test.kafka.reader.NewEventTypes.USER_WATCHED_ITEM
import java.time.ZonedDateTime

// old events
data class CreateUserEvent(val eventId: String, val type: OldEventTypes, val userId: String, val username: String, val password: String)

data class CreateLabelEvent(val eventId: String, val type: OldEventTypes, val userId: String, val labelId: String, val labelName: String)
data class AddFeedToLabel(val eventId: String, val type: OldEventTypes, val labelId: String, val feedId: String)
data class UserSubscribedToFeed(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String)
data class UserWatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)
data class UserUnwatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)

interface Event {
    fun type(): NewEventTypes
}

// new events
data class NewUserCreatedEvent(val id: String,
                               val timestamp: String,
                               val type: NewEventTypes,
                               val userId: String,
                               val username: String,
                               val password: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, userId: String, username: String, password: String) : this(id, timestamp.toString(), USER_CREATED, userId, username, password)

    override fun type() = type
}

data class NewLabelCreatedEvent(val id: String,
                                val timestamp: String,
                                val type: NewEventTypes,
                                val userId: String,
                                val labelId: String,
                                val labelName: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, userId: String, labelId: String, labelName: String) : this(id, timestamp.toString(), LABEL_CREATED, userId, labelId, labelName)

    override fun type() = type
}

data class NewFeedAddedToLabelEvent(val id: String,
                                    val timestamp: String,
                                    val type: NewEventTypes,
                                    val labelId: String,
                                    val feedId: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, labelId: String, feedId: String) : this(id, timestamp.toString(), FEED_ADDED_TO_LABEL, labelId, feedId)

    override fun type() = type
}

data class NewUserSubscribedToFeed(val id: String,
                                   val timestamp: String,
                                   val type: NewEventTypes,
                                   val userId: String,
                                   val feedId: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, userId: String, feedId: String) : this(id, timestamp.toString(), USER_SUBSCRIBED_TO_FEED, userId, feedId)

    override fun type() = type
}

data class NewUserWatchedItemEvent(val id: String,
                                   val timestamp: String,
                                   val type: NewEventTypes,
                                   val userId: String,
                                   val feedId: String,
                                   val itemId: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, userId: String, feedId: String, itemId: String) : this(id, timestamp.toString(), USER_WATCHED_ITEM, userId, feedId, itemId)

    override fun type() = type
}

data class NewUserUnwatchedItemEvent(val id: String,
                                     val timestamp: String,
                                     val type: NewEventTypes,
                                     val userId: String,
                                     val feedId: String,
                                     val itemId: String) : Event {
    constructor(id: String, timestamp: ZonedDateTime, userId: String, feedId: String, itemId: String) : this(id, timestamp.toString(), USER_UNWATCHED_ITEM, userId, feedId, itemId)

    override fun type() = type
}

enum class OldEventTypes {
    CREATE_USER,
    CHANGE_PASSWORD,
    CREATE_LABEL,
    ADD_FEED_TO_LABEL,
    USER_SUBSCRIBED_TO_FEED,
    USER_WATCHED_ITEM,
    USER_UNWATCHED_ITEM
}

enum class NewEventTypes {
    USER_CREATED,
    PASSWORD_CHANGED,
    LABEL_CREATED,
    FEED_ADDED_TO_LABEL,
    USER_SUBSCRIBED_TO_FEED,
    USER_WATCHED_ITEM,
    USER_UNWATCHED_ITEM
}
