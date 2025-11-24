package com.trendyol.quafka.common

import java.nio.charset.Charset

/**
 * The **default character set** used when decoding the value's and header's raw `ByteArray`
 * into a `String`.
 *
 * *Defaults to UTF-8* as recommended by Kafka.
 * You may override it **once at application bootstrap**:
 *
 * ```kotlin
 * DefaultCharset = Charsets.ISO_8859_1
 * ```
 */
@Suppress("ktlint:standard:property-naming")
var DefaultCharset: Charset = Charset.defaultCharset()
    @Synchronized set
