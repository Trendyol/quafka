package com.trendyol.quafka.common

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

/**
 * Removes all headers with keys matching the provided set of keys.
 *
 * @receiver A mutable list of [Header].
 * @param headers The set of header keys to remove.
 * @return The modified list of headers.
 */
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

internal fun Iterable<Header>.toQuafkaHeaders() = this.map { it.toQuafkaHeader() }

internal fun Header.toQuafkaHeader() = QuafkaHeader(this.key(), this.value())

/**
 * Converts an iterable collection of headers into a map, where the key is the header's key and the value is the [Header].
 *
 * @receiver An iterable collection of [Header].
 * @return A map of headers keyed by their key.
 */
fun Iterable<Header>.toMap(): Map<String, Header> =
    this.associateBy { it.key }

/**
 * Creates a Kafka [Header] from a string value.
 *
 * @param key The header key.
 * @param value The header value as a string.
 * @param charset The character set to use for encoding. Defaults to [DefaultCharset].
 * @return A new [Header] instance.
 */
fun header(key: String, value: String, charset: Charset = DefaultCharset): Header =
    QuafkaHeader(key, value.toByteArray(charset))

/**
 * Creates a Kafka [Header] from a numeric value.
 *
 * @param key The header key.
 * @param value The header value as a number (Int, Long, Double, etc.).
 * @param charset The character set to use for encoding.Defaults to [DefaultCharset].
 * @return A new [Header] instance.
 */
fun header(key: String, value: Number, charset: Charset = DefaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

/**
 * Creates a Kafka [Header] from a boolean value.
 *
 * @param key The header key.
 * @param value The header value as a boolean.
 * @param charset The character set to use for encoding. Defaults to [DefaultCharset].
 * @return A new [Header] instance.
 */
fun header(key: String, value: Boolean, charset: Charset = DefaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

/**
 * Creates a Kafka [Header] from an [Instant] value.
 *
 * @param key The header key.
 * @param value The header value as an [Instant].
 * @param charset The character set to use for encoding. Defaults to [DefaultCharset].
 * @return A new [Header] instance.
 */
fun header(key: String, value: Instant, charset: Charset = DefaultCharset): Header =
    QuafkaHeader(key, value.toString().toByteArray(charset))

/**
 * Creates a Kafka [Header] from a raw byte array.
 *
 * @param key The header key.
 * @param value The header value as a byte array.
 * @return A new [Header] instance.
 */
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
        charset: Charset = DefaultCharset,
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
     * @param charset Charset for decoding. Defaults to [DefaultCharset].
     * @return The decoded string, or **`null`** if the header is empty.
     */
    fun Header.asString(charset: Charset = DefaultCharset): String? =
        value()?.let { String(it, charset) }

    /**
     * Parses the header as an [Instant] using ISO-8601 format.
     *
     * @param charset Charset for decoding. Defaults to [DefaultCharset].
     * @return The parsed [Instant], or **`null`** on failure / empty header.
     */
    fun Header.asInstant(charset: Charset = DefaultCharset): Instant? =
        parse(charset, Instant::parse)

    /**
     * Converts the header to a [Boolean].
     *
     * Accepts the literal strings `"true"` / `"false"` (case-insensitive).
     *
     * @param charset Charset for decoding. Defaults to [DefaultCharset].
     * @return `true`, `false`, or **`null`** if the header is absent
     *         or not a valid boolean token.
     */
    fun Header.asBoolean(charset: Charset = DefaultCharset): Boolean? =
        parse(charset) { it.equals("true", ignoreCase = true) }

    /**
     * Converts the header to a [Long].
     *
     * @param charset Charset for decoding. Defaults to [DefaultCharset].
     * @return Parsed long value, or **`null`** on failure / empty header.
     */
    fun Header.asLong(charset: Charset = DefaultCharset): Long? =
        parse(charset, String::toLong)

    /**
     * Converts the header to an [Int].
     *
     * @param charset Charset for decoding. Defaults to [DefaultCharset].
     * @return Parsed int value, or **`null`** on failure / empty header.
     */
    fun Header.asInt(charset: Charset = DefaultCharset): Int? =
        parse(charset, String::toInt)
}
