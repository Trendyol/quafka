package com.trendyol.quafka.extensions.errorHandling.recoverer

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.producer.OutgoingMessage

typealias OutgoingMessageModifierDelegate<TKey, TValue> = suspend OutgoingMessage<TKey, TValue>.(
    message: IncomingMessage<TKey, TValue>,
    consumerContext: ConsumerContext,
    exceptionDetails: ExceptionDetails
) -> OutgoingMessage<TKey, TValue>
