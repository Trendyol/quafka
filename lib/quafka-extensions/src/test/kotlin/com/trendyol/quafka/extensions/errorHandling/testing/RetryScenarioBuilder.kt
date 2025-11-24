package com.trendyol.quafka.extensions.errorHandling.testing

import com.trendyol.quafka.extensions.errorHandling.recoverer.RetryPolicy
import kotlin.time.Duration

/**
 * DSL builder for creating declarative retry test scenarios.
 *
 * Makes retry behavior tests more readable and maintainable.
 *
 * Example:
 * ```kotlin
 * val scenario = retryScenario {
 *     policy = RetryPolicies.standardRetry(PolicyIdentifier("test"))
 *     exception = DatabaseException()
 *
 *     attempts {
 *         attempt(1) shouldRetry true withDelay 100.milliseconds
 *         attempt(2) shouldRetry true withDelay 200.milliseconds
 *         attempt(3) shouldRetry false
 *     }
 * }
 *
 * scenario.execute()
 * scenario.assertAllAttempts()
 * ```
 */
class RetryScenarioBuilder {
    var policy: RetryPolicy? = null
    var exception: Throwable? = null
    private val expectedAttempts = mutableListOf<AttemptExpectation>()

    /**
     * Expected behavior for a single retry attempt.
     */
    data class AttemptExpectation(val attemptNumber: Int, var shouldRetry: Boolean = false, var expectedDelay: Duration? = null, var description: String? = null) {
        infix fun withDelay(delay: Duration): AttemptExpectation {
            expectedDelay = delay
            return this
        }

        infix fun withDescription(desc: String): AttemptExpectation {
            description = desc
            return this
        }
    }

    /**
     * Configure retry attempt expectations.
     */
    fun attempts(block: AttemptsBuilder.() -> Unit) {
        val builder = AttemptsBuilder()
        builder.block()
        expectedAttempts.addAll(builder.build())
    }

    class AttemptsBuilder {
        private val attempts = mutableListOf<AttemptExpectation>()

        /**
         * Define expectation for a specific attempt.
         */
        fun attempt(number: Int): AttemptExpectation {
            val expectation = AttemptExpectation(number)
            attempts.add(expectation)
            return expectation
        }

        /**
         * Set whether the attempt should retry.
         */
        infix fun AttemptExpectation.shouldRetry(value: Boolean): AttemptExpectation {
            this.shouldRetry = value
            return this
        }

        fun build(): List<AttemptExpectation> = attempts
    }

    /**
     * Build the retry scenario.
     */
    fun build(): RetryScenario {
        requireNotNull(policy) { "policy must be set" }
        requireNotNull(exception) { "exception must be set" }
        return RetryScenario(policy!!, exception!!, expectedAttempts)
    }
}

/**
 * Represents a complete retry test scenario with expectations.
 */
class RetryScenario(val policy: RetryPolicy, val exception: Throwable, val expectedAttempts: List<RetryScenarioBuilder.AttemptExpectation>) {
    private val harness = RetryTestHarness()
    private val actualResults = mutableListOf<RetryTestHarness.RetryDecision>()

    /**
     * Execute the scenario by simulating the configured attempts.
     */
    fun execute() {
        actualResults.clear()
        for (expectation in expectedAttempts) {
            val decision = harness.testPolicy(policy, exception, expectation.attemptNumber)
            actualResults.add(decision)
        }
    }

    /**
     * Assert that all attempts behaved as expected.
     */
    fun assertAllAttempts() {
        require(actualResults.size == expectedAttempts.size) {
            "Expected ${expectedAttempts.size} attempts, but executed ${actualResults.size}"
        }

        for (i in expectedAttempts.indices) {
            val expected = expectedAttempts[i]
            val actual = actualResults[i]

            require(actual.shouldRetry == expected.shouldRetry) {
                "Attempt ${expected.attemptNumber}: Expected shouldRetry=${expected.shouldRetry}, " +
                    "but got ${actual.shouldRetry}. ${expected.description ?: ""}"
            }

            // Note: Delay validation would require access to the actual delay calculation
            // which is not easily testable with the current harness design
        }
    }

    /**
     * Assert that a specific attempt behaved as expected.
     */
    fun assertAttempt(attemptNumber: Int) {
        val expected = expectedAttempts.find { it.attemptNumber == attemptNumber }
            ?: throw IllegalArgumentException("No expectation for attempt $attemptNumber")

        val actual = actualResults.getOrNull(attemptNumber - 1)
            ?: throw IllegalStateException("Attempt $attemptNumber was not executed")

        require(actual.shouldRetry == expected.shouldRetry) {
            "Attempt $attemptNumber: Expected shouldRetry=${expected.shouldRetry}, " +
                "but got ${actual.shouldRetry}. ${expected.description ?: ""}"
        }
    }

    /**
     * Get the harness for advanced assertions.
     */
    fun getHarness(): RetryTestHarness = harness

    /**
     * Get all actual results.
     */
    fun getActualResults(): List<RetryTestHarness.RetryDecision> = actualResults.toList()
}

/**
 * DSL entry point for creating retry scenarios.
 *
 * Example:
 * ```kotlin
 * val scenario = retryScenario {
 *     policy = RetryPolicies.standardRetry(PolicyIdentifier("test"))
 *     exception = DatabaseException()
 *     attempts {
 *         attempt(1) shouldRetry true
 *         attempt(2) shouldRetry true
 *         attempt(3) shouldRetry false
 *     }
 * }
 * ```
 */
fun retryScenario(block: RetryScenarioBuilder.() -> Unit): RetryScenario =
    RetryScenarioBuilder().apply(block).build()
