package com.richodemus.test.kafka.reader

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test

internal class StreamMainKtTest {
    @Test
    internal fun `should convert to new format`() {
        //language=JSON
        val input = "{\"id\":\"a21bf80b-3825-4c59-9b79-1eb63a47a450\",\"type\":\"USER_WATCHED_ITEM\",\"page\":16615,\"data\":\"{\\\"userId\\\":\\\"44ad3e54-a977-4242-9d3c-bbee8187514a\\\",\\\"feedId\\\":\\\"UCjNxszyFPasDdRoD9J6X-sw\\\",\\\"itemId\\\":\\\"6ix35Fsb8qs\\\",\\\"eventId\\\":\\\"a21bf80b-3825-4c59-9b79-1eb63a47a450\\\",\\\"type\\\":\\\"USER_WATCHED_ITEM\\\"}\"}"

        //language=JSON
        val expected = "{\"id\":\"a21bf80b-3825-4c59-9b79-1eb63a47a450\",\"type\":\"USER_WATCHED_ITEM\",\"userId\":\"44ad3e54-a977-4242-9d3c-bbee8187514a\",\"feedId\":\"UCjNxszyFPasDdRoD9J6X-sw\",\"itemId\":\"6ix35Fsb8qs\"}"

        val result = transform(input)

        assertThat(result).isEqualTo(expected)
    }

    @Test
    fun `should convert some other events`() {
        listOf(
        //language=JSON
        "{\"id\":\"a21bf80b-3825-4c59-9b79-1eb63a47a450\",\"type\":\"USER_WATCHED_ITEM\",\"page\":16615,\"data\":\"{\\\"userId\\\":\\\"44ad3e54-a977-4242-9d3c-bbee8187514a\\\",\\\"feedId\\\":\\\"UCjNxszyFPasDdRoD9J6X-sw\\\",\\\"itemId\\\":\\\"6ix35Fsb8qs\\\",\\\"eventId\\\":\\\"a21bf80b-3825-4c59-9b79-1eb63a47a450\\\",\\\"type\\\":\\\"USER_WATCHED_ITEM\\\"}\"}",
        //language=JSON
        "{\"id\":\"a1bbe3df-81e8-42e1-938e-18099b5c131c\",\"type\":\"CREATE_USER\",\"page\":1,\"data\":\"{\\\"eventId\\\":\\\"a1bbe3df-81e8-42e1-938e-18099b5c131c\\\",\\\"userId\\\":\\\"44ad3e54-a977-4242-9d3c-bbee8187514a\\\",\\\"username\\\":\\\"richodemus\\\",\\\"password\\\":\\\"$2a$10\$LTNVPsQQZd3h2SmOzzzJ2uSClAdIY9.T.8crGDB4Ok2xNjrGIUPDa\\\",\\\"type\\\":\\\"CREATE_USER\\\"}\"}",
        //language=JSON
        "{\"id\":\"364cbaa6-61c9-4802-9c55-b86bfcf29923\",\"type\":\"CREATE_LABEL\",\"page\":2,\"data\":\"{\\\"eventId\\\":\\\"364cbaa6-61c9-4802-9c55-b86bfcf29923\\\",\\\"labelId\\\":\\\"e162202b-1dba-4537-a3ad-a7ee008ff86d\\\",\\\"labelName\\\":\\\"top-tier\\\",\\\"userId\\\":\\\"44ad3e54-a977-4242-9d3c-bbee8187514a\\\",\\\"type\\\":\\\"CREATE_LABEL\\\"}\"}",
        //language=JSON
        "{\"id\":\"7b901777-664c-41ff-9a8e-69a83a6c7f93\",\"type\":\"ADD_FEED_TO_LABEL\",\"page\":3,\"data\":\"{\\\"eventId\\\":\\\"7b901777-664c-41ff-9a8e-69a83a6c7f93\\\",\\\"labelId\\\":\\\"e162202b-1dba-4537-a3ad-a7ee008ff86d\\\",\\\"feedId\\\":\\\"UCtb3OOZwXGjwYMIXTu5zMKw\\\",\\\"type\\\":\\\"ADD_FEED_TO_LABEL\\\"}\"}"
        )
    }
}