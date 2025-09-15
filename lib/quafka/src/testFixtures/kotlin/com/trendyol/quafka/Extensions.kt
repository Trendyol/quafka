package com.trendyol.quafka

import com.trendyol.quafka.common.HeaderParsers.key
import com.trendyol.quafka.common.HeaderParsers.value
import com.trendyol.quafka.common.QuafkaHeader
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.*

object Extensions {
    fun Collection<Header>.toRecordHeaders(): Headers = RecordHeaders(
        this.map {
            QuafkaHeader(it.key, it.value)
        }
    )
}
