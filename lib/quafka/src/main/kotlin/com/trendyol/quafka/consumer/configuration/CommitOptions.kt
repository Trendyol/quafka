package com.trendyol.quafka.consumer.configuration

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Configuration options for controlling commit behavior in a Kafka consumer.
 *
 * This data class provides configurable options that influence how and when offsets are committed
 * to Kafka in a consumer. It enables the developer to specify the commit frequency, whether commits
 * should be deferred until all contiguous offsets are processed, if out-of-order commits are allowed,
 * and the maximum number of retry attempts on commit failures.
 *
 * @property duration The time interval between commit attempts. This determines the periodicity of commit operations.
 * Defaults to 5 seconds.
 * @property deferCommitsUntilNoGaps If `true`, commits are deferred until there are no gaps in the sequence of processed offsets,
 * ensuring a contiguous commit order. Defaults to `true`.
 * @property allowOutOfOrderCommit If `true`, allows commits to occur even if messages are processed out of order.
 * When `false`, the consumer enforces sequential commit order. Defaults to `false`.
 * @property maxRetryAttemptOnFail The maximum number of retry attempts allowed if a commit operation fails.
 * Defaults to 3.
 */
data class CommitOptions(
    val duration: Duration = 5.seconds,
    val deferCommitsUntilNoGaps: Boolean = true,
    val allowOutOfOrderCommit: Boolean = false,
    val maxRetryAttemptOnFail: Int = 3
)
