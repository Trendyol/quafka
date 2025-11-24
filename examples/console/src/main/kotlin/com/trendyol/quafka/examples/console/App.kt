package com.trendyol.quafka.examples.console

import com.trendyol.quafka.examples.console.consumers.batch.*
import com.trendyol.quafka.examples.console.consumers.single.*
import com.trendyol.quafka.examples.console.producers.*
import com.trendyol.quafka.producer.QuafkaProducer
import com.trendyol.quafka.producer.configuration.QuafkaProducerBuilder
import kotlinx.coroutines.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    App(args).run()
}

class App(private val args: Array<String>) {
    companion object {
        private val logger = LoggerFactory.getLogger(App::class.java)
    }

    fun run() {
        printWelcomeBanner()

        // Get servers from environment or prompt
        val servers = getServers()

        runBlocking {
            // Main menu
            while (true) {
                println("\n" + "=".repeat(60))
                println("QUAFKA CONSOLE - DEMO APPLICATION")
                println("=".repeat(60))
                println("1. Producer Examples")
                println("2. Consumer Examples")
                println("3. Exit")
                println("=".repeat(60))
                print("Select an option (1-3): ")

                when (readLine()?.trim()) {
                    "1" -> runProducerMenu(servers)
                    "2" -> runConsumerMenu(servers)
                    "3" -> {
                        println("\nGoodbye!")
                        exitProcess(0)
                    }

                    else -> println("\n❌ Invalid option. Please select 1, 2, or 3.")
                }
            }
        }
    }

    private fun printWelcomeBanner() {
        println("\n" + "╔" + "═".repeat(58) + "╗")
        println("║" + " ".repeat(58) + "║")
        println("║" + " ".repeat(15) + "QUAFKA DEMO APPLICATION" + " ".repeat(20) + "║")
        println("║" + " ".repeat(58) + "║")
        println("╚" + "═".repeat(58) + "╝")
    }

    private fun getServers(): String {
        val envServers = System.getenv("Q_SERVERS")
        if (!envServers.isNullOrEmpty()) {
            logger.info("Using servers from Q_SERVERS environment variable: $envServers")
            return envServers
        }

        print("\nEnter Kafka bootstrap servers (default: localhost:9092): ")
        val input = readLine()?.trim()
        return if (input.isNullOrEmpty()) "localhost:9092" else input
    }

    private fun createProducerProperties(servers: String): MutableMap<String, Any> = mutableMapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to servers
    )

    private fun createConsumerProperties(servers: String): MutableMap<String, Any> {
        val timestamp = System.currentTimeMillis()
        return mutableMapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
            ConsumerConfig.GROUP_ID_CONFIG to "quafka-demo-$timestamp",
            ConsumerConfig.MAX_POLL_RECORDS_CONFIG to 10
        )
    }

    private suspend fun runProducerMenu(servers: String) {
        while (true) {
            println("\n" + "-".repeat(60))
            println("PRODUCER EXAMPLES")
            println("-".repeat(60))
            println("1. Basic Producer - Simple message publishing")
            println("2. Batch Producer - High-throughput batch sending")
            println("3. JSON Producer - Type-safe JSON serialization")
            println("4. Header Producer - Messages with custom headers")
            println("5. Back to Main Menu")
            println("-".repeat(60))
            print("Select producer example (1-5): ")

            when (readLine()?.trim()) {
                "1" -> runProducerExample("Basic Producer") {
                    BasicProducer(createProducerProperties(servers)).run()
                }

                "2" -> runProducerExample("Batch Producer") {
                    print("Enter message count (default: 5000): ")
                    val messageCount = readLine()?.trim()?.toIntOrNull() ?: 5000
                    BatchProducer(createProducerProperties(servers), messageCount).run()
                }

                "3" -> runProducerExample("JSON Producer") {
                    JsonProducer(createProducerProperties(servers)).run()
                }

                "4" -> runProducerExample("Header Producer") {
                    HeaderProducer(createProducerProperties(servers)).run()
                }

                "5" -> return
                else -> println("\n❌ Invalid option. Please select 1-5.")
            }
        }
    }

    private suspend fun runConsumerMenu(servers: String) {
        println("\n" + "-".repeat(60))
        println("CONSUMER TYPE")
        println("-".repeat(60))
        println("1. Single Message Consumers")
        println("2. Batch Message Consumers")
        println("3. Back to Main Menu")
        println("-".repeat(60))
        print("Select consumer type (1-3): ")

        when (readLine()?.trim()) {
            "1" -> runSingleConsumerMenu(servers)
            "2" -> runBatchConsumerMenu(servers)
            "3" -> return
            else -> println("\n❌ Invalid option. Please select 1-3.")
        }
    }

    private suspend fun runSingleConsumerMenu(servers: String) {
        while (true) {
            println("\n" + "-".repeat(60))
            println("SINGLE MESSAGE CONSUMER EXAMPLES")
            println("-".repeat(60))
            println("1. Basic Consumer - Simple message processing")
            println("2. Consumer with Backpressure - Rate limiting")
            println("3. Pipelined Consumer - Middleware-based processing")
            println("4. Retryable Consumer - Advanced error handling")
            println("5. Topic Policy Based Retryable - Policy-based retry per topic")
            println("6. JSON Consumer - Type-safe JSON deserialization")
            println("7. Back to Consumer Menu")
            println("-".repeat(60))
            print("Select consumer example (1-7): ")

            when (readLine()?.trim()) {
                "1" -> runConsumerExample("Basic Consumer") {
                    BasicConsumer(createConsumerProperties(servers)).run()
                }

                "2" -> runConsumerExample("Consumer with Backpressure") {
                    BasicConsumerWithBackpressure(createConsumerProperties(servers)).run()
                }

                "3" -> {
                    val producer = createRetryProducer(servers)
                    runConsumerExample("Pipelined Consumer") {
                        PipelinedConsumer(producer, createConsumerProperties(servers)).run()
                    }
                }

                "4" -> {
                    val producer = createRetryProducer(servers)
                    runConsumerExample("Retryable Consumer") {
                        RetryableConsumer(producer, createConsumerProperties(servers)).run()
                    }
                }

                "5" -> {
                    val producer = createRetryProducer(servers)
                    runConsumerExample("Topic Policy Based Retryable Consumer") {
                        TopicPolicyBasedRetryableConsumer(producer, createConsumerProperties(servers)).run()
                    }
                }

                "6" -> runConsumerExample("JSON Consumer (Simplified API)") {
                    JsonConsumer(createConsumerProperties(servers), createProducerProperties(servers)).run()
                }

                "7" -> return
                else -> println("\n❌ Invalid option. Please select 1-7.")
            }
        }
    }

    private suspend fun runBatchConsumerMenu(servers: String) {
        while (true) {
            println("\n" + "-".repeat(60))
            println("BATCH MESSAGE CONSUMER EXAMPLES")
            println("-".repeat(60))
            println("1. Basic Batch Consumer - Simple batch processing")
            println("2. Pipelined Batch Consumer - Middleware for batches")
            println("3. Advanced Pipelined Batch - Complex workflows")
            println("4. Batch with Single Message Pipeline - Hybrid approach")
            println("5. Advanced Batch with Single Pipeline - Context sharing")
            println("6. Parallel Batch Processing - Concurrent processing")
            println("7. Flexible Batch Processing - Configurable modes")
            println("8. Concurrent with Attributes - Parallel + shared state")
            println("9. Message Result Collector - Collect processing results")
            println("10. Back to Consumer Menu")
            println("-".repeat(60))
            print("Select consumer example (1-10): ")

            when (readLine()?.trim()) {
                "1" -> runConsumerExample("Basic Batch Consumer") {
                    BasicBatchConsumer(createConsumerProperties(servers)).run()
                }

                "2" -> runConsumerExample("Pipelined Batch Consumer") {
                    PipelinedBatchConsumer(createConsumerProperties(servers)).run()
                }

                "3" -> runConsumerExample("Advanced Pipelined Batch") {
                    AdvancedPipelinedBatchConsumer(createConsumerProperties(servers)).run()
                }

                "4" -> runConsumerExample("Batch with Single Message Pipeline") {
                    BatchWithSingleMessagePipelineConsumer(createConsumerProperties(servers)).run()
                }

                "5" -> runConsumerExample("Advanced Batch with Single Pipeline") {
                    AdvancedBatchWithSingleMessagePipelineConsumer(createConsumerProperties(servers)).run()
                }

                "6" -> runConsumerExample("Parallel Batch Processing") {
                    ParallelBatchProcessingConsumer(createConsumerProperties(servers)).run()
                }

                "7" -> runConsumerExample("Flexible Batch Processing") {
                    FlexibleBatchProcessingConsumer(createConsumerProperties(servers)).run()
                }

                "8" -> runConsumerExample("Concurrent with Attributes") {
                    ConcurrentWithAttributesConsumer(createConsumerProperties(servers)).run()
                }

                "9" -> runConsumerExample("Message Result Collector") {
                    MessageResultCollectorConsumer(createConsumerProperties(servers)).run()
                }

                "10" -> return
                else -> println("\n❌ Invalid option. Please select 1-10.")
            }
        }
    }

    private suspend fun runProducerExample(name: String, example: suspend () -> Unit) {
        try {
            println("\n" + "▶".repeat(30))
            println("Running: $name")
            println("▶".repeat(30))
            example()
            println("\n✅ $name completed successfully!")
        } catch (e: Exception) {
            logger.error("❌ Error running $name", e)
            println("\n❌ Error: ${e.message}")
        }

        println("\nPress Enter to continue...")
        readLine()
    }

    private suspend fun runConsumerExample(name: String, example: suspend () -> Unit) {
        try {
            println("\n" + "▶".repeat(30))
            println("Running: $name")
            println("▶".repeat(30))
            println("\nℹ️  Consumer will run until manually stopped (Ctrl+C)")
            println("ℹ️  Make sure you have produced some messages first!")
            println("\nStarting in 3 seconds...")
            delay(3000)

            example()
        } catch (e: InterruptedException) {
            println("\n⏹️  Consumer stopped by user")
        } catch (e: Exception) {
            logger.error("❌ Error running $name", e)
            println("\n❌ Error: ${e.message}")
        }
    }

    private fun createRetryProducer(servers: String): QuafkaProducer<String, String> {
        val props = createProducerProperties(servers)

        return QuafkaProducerBuilder<String, String>(props)
            .withSerializer(StringSerializer(), StringSerializer())
            .build()
    }
}
