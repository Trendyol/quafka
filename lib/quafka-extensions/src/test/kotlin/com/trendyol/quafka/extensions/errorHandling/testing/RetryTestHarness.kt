package com.trendyol.quafka.extensions.errorHandling.testing

import com.trendyol.quafka.extensions.errorHandling.recoverer.*

/**
 * Test harness for testing retry behavior in isolation without Kafka.
 *
 * Captures retry decisions and message routing for assertion in tests.
 *
 * Example:
 * ```kotlin
 * val harness = RetryTestHarness()
 * val result = harness.testPolicy(
 *     policy = RetryPolicies.standardRetry(PolicyIdentifier("test")),
 *     exception = DatabaseException(),
 *     attempt = 1
 * )
 * assertTrue(result.shouldRetry)
 * ```
 */
class RetryTestHarness {
    private val capturedDecisions = mutableListOf<RetryDecision>()

    /**
     * Represents a retry decision made during test execution.
     */
    data class RetryDecision(val policy: RetryPolicy, val exception: Throwable, val attempt: Int, val shouldRetry: Boolean, val retryStage: RetryStage?)

    enum class RetryStage {
        IN_MEMORY,
        NON_BLOCKING
    }

    /**
     * Test a retry policy against an exception.
     *
     * @return The retry decision made by the policy
     */
    fun testPolicy(
        policy: RetryPolicy,
        exception: Throwable,
        attempt: Int = 1
    ): RetryDecision {
        val decision = when (policy) {
            RetryPolicy.NoRetry ->
                RetryDecision(policy, exception, attempt, shouldRetry = false, retryStage = null)

            is RetryPolicy.InMemory ->
                RetryDecision(
                    policy,
                    exception,
                    attempt,
                    shouldRetry = true,
                    retryStage = RetryStage.IN_MEMORY
                )

            is RetryPolicy.NonBlocking ->
                RetryDecision(
                    policy,
                    exception,
                    attempt,
                    shouldRetry = attempt <= policy.config.maxAttempts,
                    retryStage = RetryStage.NON_BLOCKING
                )

            is RetryPolicy.FullRetry ->
                RetryDecision(
                    policy,
                    exception,
                    attempt,
                    shouldRetry = true,
                    retryStage = if (attempt <= policy.inMemoryConfig.retry.retryConfig.maxAttempts) {
                        RetryStage.IN_MEMORY
                    } else {
                        RetryStage.NON_BLOCKING
                    }
                )
        }

        capturedDecisions.add(decision)
        return decision
    }

    /**
     * Get all captured retry decisions.
     */
    fun getCapturedDecisions(): List<RetryDecision> = capturedDecisions.toList()

    /**
     * Clear all captured decisions.
     */
    fun clear() {
        capturedDecisions.clear()
    }

    /**
     * Assert that a specific number of retry decisions were captured.
     */
    fun assertDecisionCount(expected: Int) {
        require(capturedDecisions.size == expected) {
            "Expected $expected decisions, but got ${capturedDecisions.size}"
        }
    }

    /**
     * Assert that the last decision was to retry.
     */
    fun assertLastShouldRetry() {
        val last = capturedDecisions.lastOrNull()
            ?: throw AssertionError("No decisions captured")
        require(last.shouldRetry) {
            "Expected last decision to retry, but it didn't"
        }
    }

    /**
     * Assert that the last decision was NOT to retry.
     */
    fun assertLastShouldNotRetry() {
        val last = capturedDecisions.lastOrNull()
            ?: throw AssertionError("No decisions captured")
        require(!last.shouldRetry) {
            "Expected last decision not to retry, but it did"
        }
    }
}
