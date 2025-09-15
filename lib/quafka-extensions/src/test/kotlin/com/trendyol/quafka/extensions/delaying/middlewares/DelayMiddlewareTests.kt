package com.trendyol.quafka.extensions.delaying.middlewares

import com.trendyol.quafka.consumer.*
import com.trendyol.quafka.extensions.consumer.single.pipelines.SingleMessageEnvelope
import com.trendyol.quafka.extensions.delaying.MessageDelayer
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.mockk.*

class DelayMiddlewareTests :
    FunSpec({

        test("should call delay") {
            val fakeDelayer = mockk<MessageDelayer>(relaxed = true)
            val incomingMessage = mockk<IncomingMessage<String, String>>(relaxed = true)
            val consumerContext = mockk<ConsumerContext>(relaxed = true)
            val singleMessageEnvelope = SingleMessageEnvelope(incomingMessage, consumerContext)
            val sut = DelayMiddleware<SingleMessageEnvelope<String, String>, String, String>(fakeDelayer)
            var executed = false
            sut.execute(singleMessageEnvelope) {
                executed = true
            }
            coVerify { fakeDelayer.delayIfNeeded(incomingMessage, consumerContext) }
            executed shouldBe true
        }
    })
