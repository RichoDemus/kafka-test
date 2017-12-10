package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.NewFeedAddedToLabelEvent
import com.richodemus.test.kafka.reader.NewLabelCreatedEvent
import com.richodemus.test.kafka.reader.NewUserCreatedEvent
import com.richodemus.test.kafka.reader.NewUserSubscribedToFeed
import com.richodemus.test.kafka.reader.NewUserUnwatchedItemEvent
import com.richodemus.test.kafka.reader.NewUserWatchedItemEvent
import org.assertj.core.api.Assertions.assertThat
import org.junit.Test
import java.time.ZonedDateTime

class JsonEventSerdeTest {
    @Test
    fun `Should deserialize into correct event`() {
        val input = listOf(
                NewUserCreatedEvent("id", ZonedDateTime.now(), "userId", "username", "password"),
                NewLabelCreatedEvent("id2", ZonedDateTime.now(), "userId", "labelId", "labelName"),
                NewFeedAddedToLabelEvent("id3", ZonedDateTime.now(), "labelId", "feedId"),
                NewUserSubscribedToFeed("id4", ZonedDateTime.now(), "userId", "feedId"),
                NewUserWatchedItemEvent("id5", ZonedDateTime.now(), "userId", "feedId", "itemId"),
                NewUserUnwatchedItemEvent("id6", ZonedDateTime.now(), "userId", "feedId", "itemId")
        )

        val result = input.map { JsonEventSerde().serializer().serialize("topic", it) }.map { JsonEventSerde().deserializer().deserialize("topic", it) }

        assertThat(result).isEqualTo(input)
    }
}
