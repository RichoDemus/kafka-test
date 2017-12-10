package com.richodemus.test.kafka.serde

import com.richodemus.test.kafka.reader.NewUserCreatedEvent
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class JsonEventSerde : Serde<NewUserCreatedEvent> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
    }

    override fun serializer(): Serializer<NewUserCreatedEvent> {
        return JsonEventSerializer()
    }

    override fun deserializer(): Deserializer<NewUserCreatedEvent> {
        return JsonEventDeserializer()
    }

    override fun close() {
    }
}

class JsonEventSerializer : Serializer<NewUserCreatedEvent> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun serialize(topic: String?, data: NewUserCreatedEvent?): ByteArray {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

class JsonEventDeserializer : Deserializer<NewUserCreatedEvent> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun deserialize(topic: String?, data: ByteArray?): NewUserCreatedEvent {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun close() {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}
