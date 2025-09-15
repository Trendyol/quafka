package com.trendyol.quafka.common

import java.net.InetAddress

internal object HostName {
    operator fun invoke(): String = InetAddress.getLocalHost().hostName
}
