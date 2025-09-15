package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.events.QuafkaEvent
import com.trendyol.quafka.producer.OutgoingMessage
import kotlin.time.Duration

object RecovererEvent {
    data class DanglingMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext
    ) : QuafkaEvent

    data class RetryMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext,
        val attempt: Int,
        val overallAttempt: Int,
        val identifier: String,
        val delay: Duration
    ) : QuafkaEvent

    data class ErrorMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext
    ) : QuafkaEvent

    data class DelayMessagePublished(
        val message: IncomingMessage<*, *>,
        val exceptionDetails: ExceptionDetails,
        val outgoingMessage: OutgoingMessage<*, *>,
        val consumerContext: ConsumerContext,
        val attempt: Int,
        val overallAttempt: Int,
        val identifier: String,
        val delay: Duration,
        val forwardingTopic: String
    ) : QuafkaEvent
}
