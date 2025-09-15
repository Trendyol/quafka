package com.trendyol.quafka.common

import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import java.nio.ByteBuffer

class QuafkaHeader : Header {
    private var inner: Header
    constructor(
        key: String,
        value: ByteArray
    ) {
        inner = RecordHeader(key, value)
    }

    constructor(key: ByteBuffer, value: ByteBuffer) {
        inner = RecordHeader(key, value)
    }

    override fun toString(): String = "key = ${key()}, value = ${String(value(), HeaderParsers.defaultCharset)}"

    override fun key(): String = inner.key()

    override fun value(): ByteArray = inner.value()

    override fun equals(o: Any?): Boolean {
        if (this === o) return true
        if (o == null || o !is Header) return false
        val header = o
        return key() == header.key() && value().contentEquals(header.value())
    }

    override fun hashCode(): Int = inner.hashCode()
}
