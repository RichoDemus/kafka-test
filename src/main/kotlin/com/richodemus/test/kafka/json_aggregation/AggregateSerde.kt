package com.richodemus.test.kafka.json_aggregation

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

private val mapper = jacksonObjectMapper()

class AggregateSerde : Serde<Aggregate> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serializer(): Serializer<Aggregate> {
        return AggregateSerializer()
    }

    override fun deserializer(): Deserializer<Aggregate> {
        return AggregateDeserializer()
    }

    override fun close() {
    }
}

class AggregateDeserializer : Deserializer<Aggregate> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun deserialize(topic: String?, data: ByteArray): Aggregate {
        return mapper.readValue(data)
    }

    override fun close() {
    }

}

class AggregateSerializer : Serializer<Aggregate> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serialize(topic: String?, data: Aggregate?): ByteArray {
        return mapper.writeValueAsBytes(data)
    }

    override fun close() {
    }

}
