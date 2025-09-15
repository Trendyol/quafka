package com.trendyol.quafka.examples.spring.models

class Job(
    val id: String,
    val payload: String,
    val offset: Long,
    val topic: String,
    val partition: Int,
    val headers: Map<String, String?>
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Job) return false

        if (id != other.id) return false

        return true
    }

    override fun hashCode(): Int = id.hashCode()
}
