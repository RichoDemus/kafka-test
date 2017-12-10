package com.richodemus.test.kafka.reader

// old events
data class CreateUserEvent(val eventId: String, val type: OldEventTypes, val userId: String, val username: String, val password: String)

data class CreateLabelEvent(val eventId: String, val type: OldEventTypes, val userId: String, val labelId: String, val labelName: String)
data class AddFeedToLabel(val eventId: String, val type: OldEventTypes, val labelId: String, val feedId: String)
data class UserSubscribedToFeed(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String)
data class UserWatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)
data class UserUnwatchedItemEvent(val eventId: String, val type: OldEventTypes, val userId: String, val feedId: String, val itemId: String)

// new events
data class NewUserCreatedEvent(val id: String,
                               val timestamp: String,
                               val type: NewEventTypes,
                               val userId: String,
                               val username: String,
                               val password: String)

data class NewLabelCreatedEvent(val id: String,
                                val timestamp: String,
                                val type: NewEventTypes,
                                val userId: String,
                                val labelId: String,
                                val labelName: String)

data class NewFeedAddedToLabelEvent(val id: String,
                                    val timestamp: String,
                                    val type: NewEventTypes,
                                    val labelId: String,
                                    val feedId: String)

data class NewUserSubscribedToFeed(val id: String,
                                   val timestamp: String,
                                   val type: NewEventTypes,
                                   val userId: String,
                                   val feedId: String)

data class NewUserWatchedItemEvent(val id: String,
                                   val timestamp: String,
                                   val type: NewEventTypes,
                                   val userId: String,
                                   val feedId: String,
                                   val itemId: String)

data class NewUserUnwatchedItemEvent(val id: String,
                                     val timestamp: String,
                                     val type: NewEventTypes,
                                     val userId: String,
                                     val feedId: String,
                                     val itemId: String)

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
