package com.richodemus.test.kafka.json_aggregation

interface Event

data class Name(val id: String, val name: String, val type: Type = Type.NAME) : Event
data class PhoneNumber(val id: String, val phoneNumber: String, val type: Type = Type.PHONE) : Event

data class Aggregate(val name: Name? = null, val phoneNumber: PhoneNumber? = null) {
    fun complete() = name != null && phoneNumber != null
    fun copy(event: Event) = when (event) {
        is Name -> this.copy(name = event)
        is PhoneNumber -> this.copy(phoneNumber = event)
        else -> this
    }
}

data class Result(val id: String, val name: String, val phoneNumber: String) {
    constructor(aggregate: Aggregate) :
            this(aggregate.name!!.id, aggregate.name.name, aggregate.phoneNumber!!.phoneNumber)
}

enum class Type {
    NAME, PHONE
}
