package com.trendyol.quafka.common

import com.trendyol.quafka.common.HeaderParsers.defaultCharset
import com.trendyol.quafka.common.HeaderParsers.key
import org.apache.kafka.common.header.Header
import java.nio.charset.*
import java.time.Instant

/**
 * Adds a header to the mutable list of headers.
 *
 * If a header with the same key already exists, it is replaced if the `override` flag is set to `true`.
 *
 * @receiver A mutable list of [Header].
 * @param header The [Header] to add.
 * @param override If `true`, replaces an existing header with the same key. Defaults to `true`.
 * @return The modified list of headers.
 */
fun MutableList<Header>.addHeader(
    header: Header,
    override: Boolean = true
): MutableList<Header> {
    val lastHeader = this.get(header.key)
    if (lastHeader != null) {
        if (!override) {
            return this
        }
        this.remove(lastHeader)
    }
    this.add(header)
    return this
}

fun MutableList<Header>.remove(headers: Set<String>): MutableList<Header> {
    this.removeAll { headers.contains(it.key) }
    return this
}

/**
 * Retrieves the last [Header] with the specified key from the iterable collection of headers.
 *
 * @receiver An iterable collection of [Header].
 * @param key The key of the header to retrieve.
 * @return The last [Header] with the specified key, or `null` if no such header exists.
 */
fun Iterable<Header>.get(key: String): Header? {
    val header = this.lastOrNull { it.key == key } ?: return null
    return header
}

fun Iterable<Header>.toQuafkaHeaders() = this.map { it.toQuafkaHeader() }

fun Header.toQuafkaHeader() = QuafkaHeader(this.key(), this.value())

/**
 * Converts an iterable collection of headers into a map, where the key is the header's key and the value is the [Header].
 *
 * @receiver An iterable collection of [Header].
 * @return A map of headers keyed by their key.
 */
fun Iterable<Header>.toMap(): Map<String, Header> =
    this.associateBy { it.key }

fun header(key: String, value: String, charset: Charset = HeaderParsers.defaultCharset): Header =
    QuafkaHeader(key, value.toByteArray(charset))

fun header(key: String, value: Number, charset: Charset = HeaderParsers.defaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

fun header(key: String, value: Boolean, charset: Charset = HeaderParsers.defaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

fun header(key: String, value: Instant, charset: Charset = HeaderParsers.defaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

fun header(key: String, value: ByteArray): Header =
    QuafkaHeader(key, value)

/**
 * Helpers for converting Kafka [Header] values to common
 * primitive and time types without boilerplate or repetitive parsing code.
 *
 * ```kotlin
 * record.headers().firstOrNull { it.key() == "retry-count" }?.asInt()
 * record.headers().lastHeader("expires-at")?.asInstant()
 * ```
 */
object HeaderParsers {
    /**
     * The **default character set** used when decoding a header’s raw `ByteArray`
     * into a `String`.
     *
     * *Defaults to UTF-8* as recommended by Kafka.
     * You may override it **once at application bootstrap**:
     *
     * ```kotlin
     * HeaderParsers.defaultCharset = Charsets.ISO_8859_1
     * ```
     */
    @JvmStatic
    var defaultCharset: Charset = StandardCharsets.UTF_8
        @Synchronized set

    /**
     * Generic parsing helper used by all public extensions.
     *
     * @receiver Header whose value will be read.
     * @param charset The charset for decoding the raw bytes.
     *                Defaults to [defaultCharset].
     * @param convert Lambda that converts the decoded `String` into the
     *                target type `T`.
     * @return Parsed value or **`null`** if the header has no value
     *         *or* conversion fails.
     */
    private inline fun <T> Header.parse(
        charset: Charset = defaultCharset,
        convert: (String) -> T
    ): T? =
        value()?.let { bytes ->
            runCatching { convert(String(bytes, charset)) }.getOrNull()
        }

    val Header.key: String get() = key()
    val Header.value: ByteArray get() = value()

    /**
     * Decodes the header value as a raw [String].
     *
     * @param charset Charset for decoding. Defaults to [defaultCharset].
     * @return The decoded string, or **`null`** if the header is empty.
     */
    fun Header.asString(charset: Charset = defaultCharset): String? =
        value()?.let { String(it, charset) }

    /**
     * Parses the header as an [Instant] using ISO-8601 format.
     *
     * @param charset Charset for decoding. Defaults to [defaultCharset].
     * @return The parsed [Instant], or **`null`** on failure / empty header.
     */
    fun Header.asInstant(charset: Charset = defaultCharset): Instant? =
        parse(charset, Instant::parse)

    /**
     * Converts the header to a [Boolean].
     *
     * Accepts the literal strings `"true"` / `"false"` (case-insensitive).
     *
     * @param charset Charset for decoding. Defaults to [defaultCharset].
     * @return `true`, `false`, or **`null`** if the header is absent
     *         or not a valid boolean token.
     */
    fun Header.asBoolean(charset: Charset = defaultCharset): Boolean? =
        parse(charset) { it.equals("true", ignoreCase = true) }

    /**
     * Converts the header to a [Long].
     *
     * @param charset Charset for decoding. Defaults to [defaultCharset].
     * @return Parsed long value, or **`null`** on failure / empty header.
     */
    fun Header.asLong(charset: Charset = defaultCharset): Long? =
        parse(charset, String::toLong)

    /**
     * Converts the header to an [Int].
     *
     * @param charset Charset for decoding. Defaults to [defaultCharset].
     * @return Parsed int value, or **`null`** on failure / empty header.
     */
    fun Header.asInt(charset: Charset = defaultCharset): Int? =
        parse(charset, String::toInt)
}
