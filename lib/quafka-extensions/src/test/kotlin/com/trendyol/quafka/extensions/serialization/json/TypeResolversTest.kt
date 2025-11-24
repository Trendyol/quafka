package com.trendyol.quafka.extensions.serialization.json

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.trendyol.quafka.IncomingMessageBuilder
import com.trendyol.quafka.common.header
import com.trendyol.quafka.consumer.IncomingMessage
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.nulls.shouldBeNull
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.header.Header

class TypeResolversTest :
    FunSpec({
        val objectMapper = ObjectMapper().registerKotlinModule()
        val jsonNodeFactory = JsonNodeFactory.instance

        context("fromHeader") {
            test("should resolve type from single header") {
                val resolver = TypeResolvers.fromHeader("X-MessageType")
                val message = createMessage(
                    headers = listOf(header("X-MessageType", "java.lang.String"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should resolve type from multiple headers in order") {
                val resolver = TypeResolvers.fromHeader(listOf("X-EventType", "X-MessageType"))
                val message = createMessage(
                    headers = listOf(
                        header("X-MessageType", "java.lang.Integer"),
                        header("X-EventType", "java.lang.String")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                // Should use the first matching header (X-EventType)
                result shouldBe String::class.java
            }

            test("should return null when header is missing") {
                val resolver = TypeResolvers.fromHeader("X-MessageType")
                val message = createMessage(headers = emptyList())
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when class is not found") {
                val resolver = TypeResolvers.fromHeader("X-MessageType")
                val message = createMessage(
                    headers = listOf(header("X-MessageType", "com.example.NonExistentClass"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should prepend package prefix") {
                val resolver = TypeResolvers.fromHeader("X-MessageType", "java.lang")
                val message = createMessage(
                    headers = listOf(header("X-MessageType", "String"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should check multiple headers and return first valid class") {
                val resolver = TypeResolvers.fromHeader(listOf("X-Type1", "X-Type2", "X-Type3"))
                val message = createMessage(
                    headers = listOf(
                        header("X-Type1", "com.example.NonExistent1"),
                        header("X-Type2", "java.lang.String"),
                        header("X-Type3", "java.lang.Integer")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                // Should return String (first valid class)
                result shouldBe String::class.java
            }
        }

        context("fromJsonField") {
            test("should resolve type from single JSON field") {
                val resolver = TypeResolvers.fromJsonField("@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "java.lang.String")

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should resolve type from multiple JSON fields in order") {
                val resolver = TypeResolvers.fromJsonField(listOf("type", "@type", "_type"))
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()
                    .put("@type", "java.lang.Integer")
                    .put("type", "java.lang.String")

                val result = resolver(jsonNode, message)

                // Should use the first matching field (type)
                result shouldBe String::class.java
            }

            test("should return null when field is missing") {
                val resolver = TypeResolvers.fromJsonField("@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when class is not found") {
                val resolver = TypeResolvers.fromJsonField("@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "com.example.NonExistentClass")

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should prepend package prefix") {
                val resolver = TypeResolvers.fromJsonField("@type", "java.lang")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "String")

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should check multiple fields and return first valid class") {
                val resolver = TypeResolvers.fromJsonField(listOf("type1", "type2", "type3"))
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()
                    .put("type1", "com.example.NonExistent1")
                    .put("type2", "java.lang.String")
                    .put("type3", "java.lang.Integer")

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }
        }

        context("fromMapping") {
            val mapping = mapOf(
                "OrderCreated" to TestOrderCreated::class.java,
                "OrderUpdated" to TestOrderUpdated::class.java
            )

            test("should resolve type from single header using mapping") {
                val resolver = TypeResolvers.fromMapping(mapping, "X-EventType")
                val message = createMessage(
                    headers = listOf(header("X-EventType", "OrderCreated"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }

            test("should resolve type from multiple headers using mapping") {
                val resolver = TypeResolvers.fromMapping(mapping, listOf("X-EventType", "X-MessageType"))
                val message = createMessage(
                    headers = listOf(
                        header("X-MessageType", "OrderUpdated"),
                        header("X-EventType", "OrderCreated")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                // Should use the first matching header (X-EventType)
                result shouldBe TestOrderCreated::class.java
            }

            test("should return null when header value is not in mapping") {
                val resolver = TypeResolvers.fromMapping(mapping, "X-EventType")
                val message = createMessage(
                    headers = listOf(header("X-EventType", "UnknownEvent"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when header is missing") {
                val resolver = TypeResolvers.fromMapping(mapping, "X-EventType")
                val message = createMessage(headers = emptyList())
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should check multiple headers and return first match in mapping") {
                val resolver = TypeResolvers.fromMapping(mapping, listOf("X-Type1", "X-Type2", "X-Type3"))
                val message = createMessage(
                    headers = listOf(
                        header("X-Type1", "UnknownType1"),
                        header("X-Type2", "OrderCreated"),
                        header("X-Type3", "OrderUpdated")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }
        }

        context("fromMappingJsonField") {
            val mapping = mapOf(
                "order.created" to TestOrderCreated::class.java,
                "order.updated" to TestOrderUpdated::class.java
            )

            test("should resolve type from single JSON field using mapping") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping, "@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "order.created")

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }

            test("should resolve type from multiple JSON fields using mapping") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping, listOf("eventType", "@type"))
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()
                    .put("@type", "order.updated")
                    .put("eventType", "order.created")

                val result = resolver(jsonNode, message)

                // Should use the first matching field (eventType)
                result shouldBe TestOrderCreated::class.java
            }

            test("should return null when field value is not in mapping") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping, "@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "unknown.event")

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when field is missing") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping, "@type")
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should use default field when not specified") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping)
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "order.created")

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }

            test("should check multiple fields and return first match in mapping") {
                val resolver = TypeResolvers.fromMappingJsonField(mapping, listOf("type1", "type2", "type3"))
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()
                    .put("type1", "unknown.type1")
                    .put("type2", "order.created")
                    .put("type3", "order.updated")

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }
        }

        context("fixed") {
            test("should always resolve to the fixed type") {
                val resolver = TypeResolvers.fixed(TestOrderCreated::class.java)
                val message = createMessage()
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }

            test("should ignore headers and JSON content") {
                val resolver = TypeResolvers.fixed(String::class.java)
                val message = createMessage(
                    headers = listOf(header("X-MessageType", "java.lang.Integer"))
                )
                val jsonNode = jsonNodeFactory.objectNode().put("@type", "java.lang.Double")

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }
        }

        context("chain") {
            test("should return first non-null result") {
                val resolver1 = TypeResolvers.fromHeader("X-Type1")
                val resolver2 = TypeResolvers.fromHeader("X-Type2")
                val resolver3 = TypeResolvers.fixed(TestOrderCreated::class.java)

                val chained = TypeResolvers.chain<String, String>(resolver1, resolver2, resolver3)

                val message = createMessage(
                    headers = listOf(header("X-Type2", "java.lang.String"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = chained(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should use fallback when all resolvers return null") {
                val resolver1 = TypeResolvers.fromHeader("X-Type1")
                val resolver2 = TypeResolvers.fromHeader("X-Type2")
                val fallback = TypeResolvers.fixed(TestOrderCreated::class.java)

                val chained = TypeResolvers.chain<String, String>(resolver1, resolver2, fallback)

                val message = createMessage(headers = emptyList())
                val jsonNode = jsonNodeFactory.objectNode()

                val result = chained(jsonNode, message)

                result shouldBe TestOrderCreated::class.java
            }

            test("should return null when all resolvers return null and no fallback") {
                val resolver1 = TypeResolvers.fromHeader("X-Type1")
                val resolver2 = TypeResolvers.fromJsonField("@type")

                val chained = TypeResolvers.chain<String, String>(resolver1, resolver2)

                val message = createMessage(headers = emptyList())
                val jsonNode = jsonNodeFactory.objectNode()

                val result = chained(jsonNode, message)

                result.shouldBeNull()
            }

            test("should combine different resolver types") {
                val headerResolver = TypeResolvers.fromHeader("X-EventType")
                val jsonFieldResolver = TypeResolvers.fromJsonField("@type")
                val mapping = mapOf("default" to TestOrderCreated::class.java)
                val mappingResolver = TypeResolvers.fromMapping(mapping, "X-MessageType")

                val chained = TypeResolvers.chain<String, String>(headerResolver, jsonFieldResolver, mappingResolver)

                // Test that JSON field resolver works when header is missing
                val message1 = createMessage(headers = emptyList())
                val jsonNode1 = jsonNodeFactory.objectNode().put("@type", "java.lang.String")
                chained(jsonNode1, message1) shouldBe String::class.java

                // Test that mapping resolver works when both header and JSON field are missing
                val message2 = createMessage(
                    headers = listOf(header("X-MessageType", "default"))
                )
                val jsonNode2 = jsonNodeFactory.objectNode()
                chained(jsonNode2, message2) shouldBe TestOrderCreated::class.java
            }
        }

        context("fromPackageAndName") {
            test("should resolve type from single type and package headers") {
                val resolver = TypeResolvers.fromPackageAndName("typeName", "packageName")
                val message = createMessage(
                    headers = listOf(
                        header("typeName", "String"),
                        header("packageName", "java.lang")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }

            test("should resolve type from multiple type and package headers") {
                val resolver = TypeResolvers.fromPackageAndName(
                    listOf("type1", "type2"),
                    listOf("package1", "package2")
                )
                val message = createMessage(
                    headers = listOf(
                        // type1 is missing, so it should use type2
                        header("type2", "String"),
                        // package1 is missing, so it should use package2
                        header("package2", "java.lang")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                // Should use first available type (type2) and first available package (package2)
                result shouldBe String::class.java
            }

            test("should return null when type header is missing") {
                val resolver = TypeResolvers.fromPackageAndName("typeName", "packageName")
                val message = createMessage(
                    headers = listOf(header("packageName", "java.lang"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when package header is missing") {
                val resolver = TypeResolvers.fromPackageAndName("typeName", "packageName")
                val message = createMessage(
                    headers = listOf(header("typeName", "String"))
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should return null when class is not found") {
                val resolver = TypeResolvers.fromPackageAndName("typeName", "packageName")
                val message = createMessage(
                    headers = listOf(
                        header("typeName", "NonExistentClass"),
                        header("packageName", "com.example")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result.shouldBeNull()
            }

            test("should use default headers when not specified") {
                val resolver = TypeResolvers.fromPackageAndName()
                val message = createMessage(
                    headers = listOf(
                        header("type", "String"),
                        header("package", "java.lang")
                    )
                )
                val jsonNode = jsonNodeFactory.objectNode()

                val result = resolver(jsonNode, message)

                result shouldBe String::class.java
            }
        }

        context("integration scenarios") {
            test("should handle complex chaining with multiple fallbacks") {
                val mapping = mapOf("fallback" to TestOrderCreated::class.java)
                val resolver = TypeResolvers.chain<String, String>(
                    TypeResolvers.fromHeader(listOf("X-Type1", "X-Type2")),
                    TypeResolvers.fromJsonField(listOf("type", "@type")),
                    TypeResolvers.fromMapping(mapping, listOf("X-Fallback1", "X-Fallback2")),
                    TypeResolvers.fixed(String::class.java)
                )

                // Scenario 1: First header resolver succeeds
                val message1 = createMessage(headers = listOf(header("X-Type2", "java.lang.Integer")))
                val jsonNode1 = jsonNodeFactory.objectNode()
                resolver(jsonNode1, message1) shouldBe Integer::class.java

                // Scenario 2: JSON field resolver succeeds
                val message2 = createMessage(headers = emptyList())
                val jsonNode2 = jsonNodeFactory.objectNode().put("@type", "java.lang.Double")
                resolver(jsonNode2, message2) shouldBe java.lang.Double::class.java

                // Scenario 3: Mapping resolver succeeds
                val message3 = createMessage(headers = listOf(header("X-Fallback2", "fallback")))
                val jsonNode3 = jsonNodeFactory.objectNode()
                resolver(jsonNode3, message3) shouldBe TestOrderCreated::class.java

                // Scenario 4: Fixed fallback is used
                val message4 = createMessage(headers = emptyList())
                val jsonNode4 = jsonNodeFactory.objectNode()
                resolver(jsonNode4, message4) shouldBe String::class.java
            }

            test("should work with real world JSON messages") {
                val mapping = mapOf(
                    "OrderCreatedEvent" to TestOrderCreated::class.java,
                    "OrderUpdatedEvent" to TestOrderUpdated::class.java
                )

                val resolver = TypeResolvers.chain<String, String>(
                    TypeResolvers.fromHeader("X-Event-Type", "com.trendyol.quafka.extensions.serialization.json"),
                    TypeResolvers.fromMappingJsonField(mapping, "eventType")
                )

                // Test with header-based resolution
                val message1 = createMessage(
                    headers = listOf(header("X-Event-Type", "TestOrderCreated"))
                )
                val json1 = objectMapper.readTree("""{"orderId": "123"}""")
                resolver(json1, message1) shouldBe TestOrderCreated::class.java

                // Test with JSON field-based resolution
                val message2 = createMessage(headers = emptyList())
                val json2 = objectMapper.readTree("""{"eventType": "OrderCreatedEvent", "orderId": "456"}""")
                resolver(json2, message2) shouldBe TestOrderCreated::class.java
            }
        }
    })

// Helper function to create test messages
private fun createMessage(
    key: String = "test-key",
    value: String = "test-value",
    headers: List<org.apache.kafka.common.header.Header> = emptyList()
): IncomingMessage<String, String> = IncomingMessageBuilder<String, String>(
    topic = "test-topic",
    partition = 0,
    offset = 0L,
    key = key,
    value = value,
    headers = headers.toMutableList()
).build()

// Test data classes
private data class TestOrderCreated(val orderId: String = "")
private data class TestOrderUpdated(val orderId: String = "", val status: String = "")
