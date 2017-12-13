package com.richodemus.test.kafka.json_aggregation

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.richodemus.test.kafka.json_aggregation.Type.NAME
import com.richodemus.test.kafka.json_aggregation.Type.PHONE
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

private val mapper = jacksonObjectMapper()

class EventSerde : Serde<Event> {

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serializer(): Serializer<Event> {
        return EventSerializer()
    }

    override fun deserializer(): Deserializer<Event> {
        return object : Deserializer<Event> {
            override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
            }

            override fun deserialize(topic: String?, data: ByteArray): Event {
                val str = String(data)
                return when {
                    str isType NAME -> mapper.readValue<Name>(data)
                    str isType PHONE -> mapper.readValue<PhoneNumber>(data)
                    else -> throw IllegalStateException()
                }

            }

            override fun close() {
            }

            private infix fun String.isType(type: Type) = type.toString() in this
        }
    }

    override fun close() {
    }
}

class EventSerializer : Serializer<Event> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serialize(topic: String?, data: Event): ByteArray {
        return mapper.writeValueAsBytes(data)
    }

    override fun close() {
    }

}
