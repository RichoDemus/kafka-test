package com.richodemus.test.kafka.ktable

import org.junit.Test
import java.util.regex.Pattern

class ReaderDataTableMainKtTest{
    @Test
    fun `Test regexp`() {
        //language=JSON
        val json = "{\"id\":\"09b81ae7-0985-42eb-a797-121c22e195fb\",\"timestamp\":\"2017-12-09T11:13:18.085Z\",\"type\":\"LABEL_CREATED\",\"userId\":\"44ad3e54-a977-4242-9d3c-bbee8187514a\",\"labelId\":\"387fa4f4-3371-4e86-873f-c3ad1c1bd01e\",\"labelName\":\"Gaming\"}"

        //language=REGEXP
        val stringPattern = "\"id\":\"(.*?)\""
        val pattern = Pattern.compile(stringPattern)

        val matcher = pattern.matcher(json)

        println(matcher.find())
        println(matcher.group(0))

    }
}