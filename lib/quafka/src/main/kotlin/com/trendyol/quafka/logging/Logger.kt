package com.trendyol.quafka.logging

import org.slf4j.*
import org.slf4j.event.Level
import org.slf4j.spi.*

internal class QuafkaLogger(private val logger: Logger, private val keyValuePairs: Map<String, Any> = mapOf()) : Logger by logger {
    override fun equals(other: Any?): Boolean = super.equals(other)

    override fun hashCode(): Int = super.hashCode()

    override fun toString(): String = super.toString()

    override fun getName(): String = logger.name

    override fun makeLoggingEventBuilder(level: Level?): LoggingEventBuilder = logger.makeLoggingEventBuilder(level)

    override fun atLevel(level: Level?): LoggingEventBuilder {
        if (!logger.isEnabledForLevel(level)) {
            return NOPLoggingEventBuilder.singleton()
        }
        return logger.atLevel(level).apply {
            keyValuePairs.forEach { (k, v) -> this.addKeyValue(k, v) }
        }
    }

    override fun isEnabledForLevel(level: Level?): Boolean = logger.isEnabledForLevel(level)

    override fun isTraceEnabled(marker: Marker?): Boolean = logger.isTraceEnabled(marker)

    override fun isTraceEnabled(): Boolean = logger.isTraceEnabled

    override fun trace(
        marker: Marker,
        msg: String
    ) {
        atTrace().addMarker(marker).log(msg)
    }

    override fun trace(
        format: String,
        vararg arguments: Any?
    ) {
        atTrace().log(format, *arguments)
    }

    override fun trace(
        msg: String?,
        t: Throwable?
    ) {
        atTrace().setCause(t).log(msg)
    }

    override fun trace(
        marker: Marker?,
        format: String?,
        vararg argArray: Any?
    ) {
        atTrace().addMarker(marker).log(format, *argArray)
    }

    override fun trace(
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atTrace().log(format, arg1, arg2)
    }

    override fun trace(
        marker: Marker?,
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atTrace().addMarker(marker).log(format, arg1, arg2)
    }

    override fun trace(
        format: String?,
        arg: Any?
    ) {
        atTrace().log(format, arg)
    }

    override fun trace(msg: String?) {
        atTrace().log(msg)
    }

    override fun trace(
        marker: Marker?,
        msg: String?,
        t: Throwable?
    ) {
        atTrace().addMarker(marker).setCause(t).log(msg)
    }

    override fun trace(
        marker: Marker?,
        format: String?,
        arg: Any?
    ) {
        atTrace().addMarker(marker).log(format, arg)
    }

    override fun atTrace(): LoggingEventBuilder = atLevel(Level.TRACE)

    override fun isDebugEnabled(): Boolean = logger.isDebugEnabled

    override fun isDebugEnabled(marker: Marker?): Boolean = logger.isDebugEnabled(marker)

    override fun debug(
        marker: Marker,
        msg: String
    ) {
        atDebug().addMarker(marker).log(msg)
    }

    override fun debug(
        format: String,
        vararg arguments: Any?
    ) {
        atDebug().log(format, *arguments)
    }

    override fun debug(
        msg: String?,
        t: Throwable?
    ) {
        atDebug().setCause(t).log(msg)
    }

    override fun debug(
        marker: Marker?,
        format: String?,
        vararg argArray: Any?
    ) {
        atDebug().addMarker(marker).log(format, *argArray)
    }

    override fun debug(
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atDebug().log(format, arg1, arg2)
    }

    override fun debug(
        marker: Marker?,
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atDebug().addMarker(marker).log(format, arg1, arg2)
    }

    override fun debug(
        format: String?,
        arg: Any?
    ) {
        atDebug().log(format, arg)
    }

    override fun debug(msg: String?) {
        atDebug().log(msg)
    }

    override fun debug(
        marker: Marker?,
        msg: String?,
        t: Throwable?
    ) {
        atDebug().addMarker(marker).setCause(t).log(msg)
    }

    override fun debug(
        marker: Marker?,
        format: String?,
        arg: Any?
    ) {
        atDebug().addMarker(marker).log(format, arg)
    }

    override fun atDebug(): LoggingEventBuilder = atLevel(Level.DEBUG)

    override fun isInfoEnabled(): Boolean = logger.isInfoEnabled

    override fun isInfoEnabled(marker: Marker?): Boolean = logger.isInfoEnabled(marker)

    override fun info(
        marker: Marker,
        msg: String
    ) {
        atInfo().addMarker(marker).log(msg)
    }

    override fun info(
        format: String,
        vararg arguments: Any?
    ) {
        atInfo().log(format, *arguments)
    }

    override fun info(
        msg: String?,
        t: Throwable?
    ) {
        atInfo().setCause(t).log(msg)
    }

    override fun info(
        marker: Marker?,
        format: String?,
        vararg argArray: Any?
    ) {
        atInfo().addMarker(marker).log(format, *argArray)
    }

    override fun info(
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atInfo().log(format, arg1, arg2)
    }

    override fun info(
        marker: Marker?,
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atInfo().addMarker(marker).log(format, arg1, arg2)
    }

    override fun info(
        format: String?,
        arg: Any?
    ) {
        atInfo().log(format, arg)
    }

    override fun info(msg: String?) {
        atInfo().log(msg)
    }

    override fun info(
        marker: Marker?,
        msg: String?,
        t: Throwable?
    ) {
        atInfo().addMarker(marker).setCause(t).log(msg)
    }

    override fun info(
        marker: Marker?,
        format: String?,
        arg: Any?
    ) {
        atInfo().addMarker(marker).log(format, arg)
    }

    override fun atInfo(): LoggingEventBuilder = atLevel(Level.INFO)

    override fun isWarnEnabled(): Boolean = logger.isWarnEnabled

    override fun isWarnEnabled(marker: Marker?): Boolean = logger.isWarnEnabled(marker)

    override fun warn(
        marker: Marker,
        msg: String
    ) {
        atWarn().addMarker(marker).log(msg)
    }

    override fun warn(
        format: String,
        vararg arguments: Any?
    ) {
        atWarn().log(format, *arguments)
    }

    override fun warn(
        msg: String?,
        t: Throwable?
    ) {
        atWarn().setCause(t).log(msg)
    }

    override fun warn(
        marker: Marker?,
        format: String?,
        vararg argArray: Any?
    ) {
        atWarn().addMarker(marker).log(format, *argArray)
    }

    override fun warn(
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atWarn().log(format, arg1, arg2)
    }

    override fun warn(
        marker: Marker?,
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atWarn().addMarker(marker).log(format, arg1, arg2)
    }

    override fun warn(
        format: String?,
        arg: Any?
    ) {
        atWarn().log(format, arg)
    }

    override fun warn(msg: String?) {
        atWarn().log(msg)
    }

    override fun warn(
        marker: Marker?,
        msg: String?,
        t: Throwable?
    ) {
        atWarn().addMarker(marker).setCause(t).log(msg)
    }

    override fun warn(
        marker: Marker?,
        format: String?,
        arg: Any?
    ) {
        atWarn().addMarker(marker).log(format, arg)
    }

    override fun atWarn(): LoggingEventBuilder = atLevel(Level.WARN)

    override fun isErrorEnabled(): Boolean = logger.isErrorEnabled

    override fun isErrorEnabled(marker: Marker?): Boolean = logger.isErrorEnabled(marker)

    override fun error(
        marker: Marker,
        msg: String
    ) {
        atError().addMarker(marker).log(msg)
    }

    override fun error(
        format: String,
        vararg arguments: Any?
    ) {
        atError().log(format, *arguments)
    }

    override fun error(
        msg: String?,
        t: Throwable?
    ) {
        atError().setCause(t).log(msg)
    }

    override fun error(
        marker: Marker?,
        format: String?,
        vararg argArray: Any?
    ) {
        atError().addMarker(marker).log(format, *argArray)
    }

    override fun error(
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atError().log(format, arg1, arg2)
    }

    override fun error(
        marker: Marker?,
        format: String?,
        arg1: Any?,
        arg2: Any?
    ) {
        atError().addMarker(marker).log(format, arg1, arg2)
    }

    override fun error(
        format: String?,
        arg: Any?
    ) {
        atError().log(format, arg)
    }

    override fun error(msg: String?) {
        atError().log(msg)
    }

    override fun error(
        marker: Marker?,
        msg: String?,
        t: Throwable?
    ) {
        atError().addMarker(marker).setCause(t).log(msg)
    }

    override fun error(
        marker: Marker?,
        format: String?,
        arg: Any?
    ) {
        atError().addMarker(marker).log(format, arg)
    }

    override fun atError(): LoggingEventBuilder = atLevel(Level.ERROR)
}
