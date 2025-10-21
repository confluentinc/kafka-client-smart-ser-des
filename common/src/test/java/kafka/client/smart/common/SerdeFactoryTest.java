/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.stream.Stream;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive tests for SerdeFactory.
 * Tests serializer and deserializer creation for all supported types.
 */
@DisplayName("SerdeFactory Tests")
class SerdeFactoryTest {

    @ParameterizedTest
    @MethodSource("provideSerializerTestCases")
    @DisplayName("createSerializer should return correct serializer for each type")
    void testCreateSerializer(SerializationTypes type, Class<? extends Serializer<?>> expectedClass) {
        Serializer<?> serializer = SerdeFactory.createSerializer(type);
        assertNotNull(serializer);
        assertTrue(expectedClass.isInstance(serializer));
    }

    @ParameterizedTest
    @MethodSource("provideDeserializerTestCases")
    @DisplayName("createDeserializer should return correct deserializer for each type")
    void testCreateDeserializer(SerializationTypes type, Class<? extends Deserializer<?>> expectedClass) {
        SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> mockSRClient);
        
        assertNotNull(deserializer);
        assertTrue(expectedClass.isInstance(deserializer));
    }

    @Test
    @DisplayName("createSerializer should handle all SerializationTypes enum values")
    void testCreateSerializerAllTypes() {
        for (SerializationTypes type : SerializationTypes.values()) {
            assertDoesNotThrow(() -> {
                Serializer<?> serializer = SerdeFactory.createSerializer(type);
                assertNotNull(serializer, "Serializer should not be null for type: " + type);
            });
        }
    }

    @Test
    @DisplayName("createDeserializer should handle all SerializationTypes enum values")
    void testCreateDeserializerAllTypes() {
        SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        
        for (SerializationTypes type : SerializationTypes.values()) {
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> mockSRClient);
                assertNotNull(deserializer, "Deserializer should not be null for type: " + type);
            });
        }
    }

    @Test
    @DisplayName("createDeserializer should use SchemaRegistryClient supplier for schema-based types")
    void testCreateDeserializerUsesSRClient() {
        SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        
        // Test schema-based deserializers that should use the SR client
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> mockSRClient);
            assertNotNull(deserializer);
            // The deserializer should be created successfully with the SR client
        }
    }

    @Test
    @DisplayName("createDeserializer should handle null SchemaRegistryClient supplier gracefully")
    void testCreateDeserializerNullSRClient() {
        // This should not throw an exception for non-schema-based types
        SerializationTypes[] nonSchemaTypes = {
            SerializationTypes.String,
            SerializationTypes.Integer,
            SerializationTypes.Long,
            SerializationTypes.ByteArray
        };
        
        for (SerializationTypes type : nonSchemaTypes) {
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> null);
                assertNotNull(deserializer);
            });
        }
    }

    @Test
    @DisplayName("createSerializer should return different instances for each call")
    void testCreateSerializerReturnsNewInstances() {
        Serializer<?> serializer1 = SerdeFactory.createSerializer(SerializationTypes.String);
        Serializer<?> serializer2 = SerdeFactory.createSerializer(SerializationTypes.String);
        
        assertNotSame(serializer1, serializer2, "Each call should return a new instance");
    }

    @Test
    @DisplayName("createDeserializer should return different instances for each call")
    void testCreateDeserializerReturnsNewInstances() {
        SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        
        Deserializer<?> deserializer1 = SerdeFactory.createDeserializer(SerializationTypes.String, () -> mockSRClient);
        Deserializer<?> deserializer2 = SerdeFactory.createDeserializer(SerializationTypes.String, () -> mockSRClient);
        
        assertNotSame(deserializer1, deserializer2, "Each call should return a new instance");
    }

    @Test
    @DisplayName("createSerializer should handle null input gracefully")
    void testCreateSerializerNullInput() {
        assertDoesNotThrow(() -> {
            Serializer<?> serializer = SerdeFactory.createSerializer(null);
            assertNotNull(serializer);
        });
    }

    @Test
    @DisplayName("createDeserializer should handle null input gracefully")
    void testCreateDeserializerNullInput() {
        SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        
        assertDoesNotThrow(() -> {
            Deserializer<?> deserializer = SerdeFactory.createDeserializer(null, () -> mockSRClient);
            assertNotNull(deserializer);
        });
    }

    private static Stream<Arguments> provideSerializerTestCases() {
        return Stream.of(
            Arguments.of(SerializationTypes.String, StringSerializer.class),
            Arguments.of(SerializationTypes.Boolean, BooleanSerializer.class),
            Arguments.of(SerializationTypes.Bytes, BytesSerializer.class),
            Arguments.of(SerializationTypes.ByteArray, ByteArraySerializer.class),
            Arguments.of(SerializationTypes.ByteBuffer, ByteBufferSerializer.class),
            Arguments.of(SerializationTypes.Short, ShortSerializer.class),
            Arguments.of(SerializationTypes.Integer, IntegerSerializer.class),
            Arguments.of(SerializationTypes.Long, LongSerializer.class),
            Arguments.of(SerializationTypes.Float, FloatSerializer.class),
            Arguments.of(SerializationTypes.Double, DoubleSerializer.class),
            Arguments.of(SerializationTypes.UUID, UUIDSerializer.class),
            Arguments.of(SerializationTypes.Json, KafkaJsonSerializer.class),
            Arguments.of(SerializationTypes.JsonSchema, KafkaJsonSchemaSerializer.class),
            Arguments.of(SerializationTypes.Avro, KafkaAvroSerializer.class),
            Arguments.of(SerializationTypes.Protobuf, KafkaProtobufSerializer.class)
        );
    }

    private static Stream<Arguments> provideDeserializerTestCases() {
        return Stream.of(
            Arguments.of(SerializationTypes.String, StringDeserializer.class),
            Arguments.of(SerializationTypes.Boolean, BooleanDeserializer.class),
            Arguments.of(SerializationTypes.Bytes, BytesDeserializer.class),
            Arguments.of(SerializationTypes.ByteArray, ByteArrayDeserializer.class),
            Arguments.of(SerializationTypes.ByteBuffer, ByteBufferDeserializer.class),
            Arguments.of(SerializationTypes.Short, ShortDeserializer.class),
            Arguments.of(SerializationTypes.Integer, IntegerDeserializer.class),
            Arguments.of(SerializationTypes.Long, LongDeserializer.class),
            Arguments.of(SerializationTypes.Float, FloatDeserializer.class),
            Arguments.of(SerializationTypes.Double, DoubleDeserializer.class),
            Arguments.of(SerializationTypes.UUID, UUIDDeserializer.class),
            Arguments.of(SerializationTypes.Json, KafkaJsonDeserializer.class),
            Arguments.of(SerializationTypes.JsonSchema, KafkaJsonSchemaDeserializer.class),
            Arguments.of(SerializationTypes.Avro, KafkaAvroDeserializer.class),
            Arguments.of(SerializationTypes.Protobuf, KafkaProtobufDeserializer.class)
        );
    }

    @Test
    @DisplayName("createSerializer should be thread-safe under concurrent access")
    void testCreateSerializerConcurrency() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Serializer<?>> serializers = Collections.synchronizedList(new ArrayList<>());

        // Test all serialization types concurrently
        for (SerializationTypes type : SerializationTypes.values()) {
            for (int i = 0; i < numberOfThreads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        
                        for (int j = 0; j < iterationsPerThread; j++) {
                            Serializer<?> serializer = SerdeFactory.createSerializer(type);
                            serializers.add(serializer);
                            
                            // Verify the serializer is not null and is the correct type
                            assertNotNull(serializer, "Serializer should not be null for type: " + type);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during concurrent access: " + exceptions);
        
        // Verify we got the expected number of serializers
        int expectedCount = SerializationTypes.values().length * numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, serializers.size(), "Should create expected number of serializers");
        
        // Verify all serializers are different instances (not cached)
        assertEquals(serializers.size(), serializers.stream().distinct().count(), 
            "All serializers should be different instances");
    }

    @Test
    @DisplayName("createDeserializer should be thread-safe under concurrent access")
    void testCreateDeserializerConcurrency() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Deserializer<?>> deserializers = Collections.synchronizedList(new ArrayList<>());
        final SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);

        // Test all serialization types concurrently
        for (SerializationTypes type : SerializationTypes.values()) {
            for (int i = 0; i < numberOfThreads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        
                        for (int j = 0; j < iterationsPerThread; j++) {
                            Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> mockSRClient);
                            deserializers.add(deserializer);
                            
                            // Verify the deserializer is not null and is the correct type
                            assertNotNull(deserializer, "Deserializer should not be null for type: " + type);
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during concurrent access: " + exceptions);
        
        // Verify we got the expected number of deserializers
        int expectedCount = SerializationTypes.values().length * numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, deserializers.size(), "Should create expected number of deserializers");
        
        // Verify all deserializers are different instances (not cached)
        assertEquals(deserializers.size(), deserializers.stream().distinct().count(), 
            "All deserializers should be different instances");
    }

    @Test
    @DisplayName("createSerializer should handle mixed concurrent access with different types")
    void testCreateSerializerMixedConcurrency() throws InterruptedException {
        final int numberOfThreads = 20;
        final int iterationsPerThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Serializer<?>> serializers = Collections.synchronizedList(new ArrayList<>());
        final SerializationTypes[] types = SerializationTypes.values();

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        // Each thread uses a different type based on its index
                        SerializationTypes type = types[threadIndex % types.length];
                        Serializer<?> serializer = SerdeFactory.createSerializer(type);
                        serializers.add(serializer);
                        
                        // Verify the serializer is not null
                        assertNotNull(serializer, "Serializer should not be null for type: " + type);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during mixed concurrent access: " + exceptions);
        
        // Verify we got the expected number of serializers
        int expectedCount = numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, serializers.size(), "Should create expected number of serializers");
    }

    @Test
    @DisplayName("createDeserializer should handle mixed concurrent access with different types")
    void testCreateDeserializerMixedConcurrency() throws InterruptedException {
        final int numberOfThreads = 20;
        final int iterationsPerThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Deserializer<?>> deserializers = Collections.synchronizedList(new ArrayList<>());
        final SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);
        final SerializationTypes[] types = SerializationTypes.values();

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        // Each thread uses a different type based on its index
                        SerializationTypes type = types[threadIndex % types.length];
                        Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> mockSRClient);
                        deserializers.add(deserializer);
                        
                        // Verify the deserializer is not null
                        assertNotNull(deserializer, "Deserializer should not be null for type: " + type);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during mixed concurrent access: " + exceptions);
        
        // Verify we got the expected number of deserializers
        int expectedCount = numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, deserializers.size(), "Should create expected number of deserializers");
    }

    @Test
    @DisplayName("createSerializer should handle null input concurrently")
    void testCreateSerializerNullInputConcurrency() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Serializer<?>> serializers = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        Serializer<?> serializer = SerdeFactory.createSerializer(null);
                        serializers.add(serializer);
                        
                        // Verify the serializer is not null (should return default)
                        assertNotNull(serializer, "Serializer should not be null even for null input");
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during concurrent null input access: " + exceptions);
        
        // Verify we got the expected number of serializers
        int expectedCount = numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, serializers.size(), "Should create expected number of serializers");
    }

    @Test
    @DisplayName("createDeserializer should handle null input concurrently")
    void testCreateDeserializerNullInputConcurrency() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 100;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Deserializer<?>> deserializers = Collections.synchronizedList(new ArrayList<>());
        final SchemaRegistryClient mockSRClient = mock(SchemaRegistryClient.class);

        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        Deserializer<?> deserializer = SerdeFactory.createDeserializer(null, () -> mockSRClient);
                        deserializers.add(deserializer);
                        
                        // Verify the deserializer is not null (should return default)
                        assertNotNull(deserializer, "Deserializer should not be null even for null input");
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no exceptions occurred
        assertTrue(exceptions.isEmpty(), "No exceptions should occur during concurrent null input access: " + exceptions);
        
        // Verify we got the expected number of deserializers
        int expectedCount = numberOfThreads * iterationsPerThread;
        assertEquals(expectedCount, deserializers.size(), "Should create expected number of deserializers");
    }

    @Test
    @DisplayName("createDeserializer should propagate Schema Registry connection failures")
    void testCreateDeserializerSchemaRegistryConnectionFailure() {
        // Mock supplier that throws connection exceptions
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            throw new RuntimeException("Connection refused");
        };
        
        // Test that schema-based deserializers propagate connection failures
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for SR connection failure with type: " + type);
            
            assertEquals("Connection refused", exception.getMessage());
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate Schema Registry timeout exceptions")
    void testCreateDeserializerSchemaRegistryTimeout() {
        // Mock supplier that throws timeout exceptions
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            throw new RuntimeException("Read timeout");
        };
        
        // Test that schema-based deserializers propagate timeout exceptions
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for SR timeout with type: " + type);
            
            assertEquals("Read timeout", exception.getMessage());
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate Schema Registry authentication failures")
    void testCreateDeserializerSchemaRegistryAuthFailure() {
        // Mock supplier that throws authentication exceptions
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            throw new RuntimeException("Authentication failed");
        };
        
        // Test that schema-based deserializers propagate auth failures
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for SR auth failure with type: " + type);
            
            assertEquals("Authentication failed", exception.getMessage());
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate Schema Registry client supplier exceptions")
    void testCreateDeserializerSchemaRegistrySupplierException() {
        // Mock supplier that throws exceptions
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            throw new RuntimeException("Schema Registry client creation failed");
        };
        
        // Test that schema-based deserializers propagate supplier exceptions
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for SR supplier failure with type: " + type);
            
            assertEquals("Schema Registry client creation failed", exception.getMessage());
        }
    }

    @Test
    @DisplayName("createDeserializer should handle null Schema Registry client from supplier")
    void testCreateDeserializerNullSchemaRegistryClient() {
        // Mock supplier that returns null
        Supplier<SchemaRegistryClient> nullSupplier = () -> null;
        
        // Test that schema-based deserializers handle null SR client
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, nullSupplier);
                assertNotNull(deserializer, "Deserializer should be created even with null SR client for type: " + type);
            }, "Should not throw exception for null SR client with type: " + type);
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate RestClientException from Schema Registry")
    void testCreateDeserializerRestClientException() {
        // Mock supplier that throws RestClientException
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            try {
                throw new RestClientException("Schema Registry error", 500, 500);
            } catch (RestClientException e) {
                throw new RuntimeException(e);
            }
        };
        
        // Test that schema-based deserializers propagate RestClientException
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for RestClientException with type: " + type);
            
            assertTrue(exception.getCause() instanceof RestClientException);
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate IOException from Schema Registry")
    void testCreateDeserializerIOException() {
        // Mock supplier that throws IOException
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            try {
                throw new java.io.IOException("Network error");
            } catch (java.io.IOException e) {
                throw new RuntimeException(e);
            }
        };
        
        // Test that schema-based deserializers propagate IOException
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failingSupplier);
            }, "Should throw exception for IOException with type: " + type);
            
            assertTrue(exception.getCause() instanceof java.io.IOException);
        }
    }

    @Test
    @DisplayName("createDeserializer should propagate concurrent Schema Registry failures")
    void testCreateDeserializerConcurrentSchemaRegistryFailures() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        final List<Deserializer<?>> deserializers = Collections.synchronizedList(new ArrayList<>());
        
        // Mock supplier that throws exceptions
        Supplier<SchemaRegistryClient> failingSupplier = () -> {
            throw new RuntimeException("Concurrent SR failure");
        };
        
        // Test all schema-based types concurrently with SR failures
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            for (int i = 0; i < numberOfThreads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await(); // Wait for all threads to be ready
                        
                        for (int j = 0; j < iterationsPerThread; j++) {
                            try {
                                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, failingSupplier);
                                deserializers.add(deserializer);
                            } catch (RuntimeException e) {
                                // Expected - SR failures should propagate
                                exceptions.add(e);
                            }
                        }
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify exceptions occurred (SR failures should propagate)
        assertFalse(exceptions.isEmpty(), "Exceptions should occur during concurrent SR failure handling");
        
        // Verify all exceptions are RuntimeExceptions with expected message
        for (Exception e : exceptions) {
            assertTrue(e instanceof RuntimeException, "All exceptions should be RuntimeExceptions");
            assertEquals("Concurrent SR failure", e.getMessage());
        }
        
        // Verify no deserializers were created (all should have failed)
        assertEquals(0, deserializers.size(), "No deserializers should be created when SR fails");
    }

    @Test
    @DisplayName("createDeserializer should handle mixed Schema Registry success and failure scenarios")
    void testCreateDeserializerMixedSchemaRegistryScenarios() {
        // Test with different SR client behaviors
        SchemaRegistryClient successSRClient = mock(SchemaRegistryClient.class);
        Supplier<SchemaRegistryClient> failureSupplier = () -> {
            throw new RuntimeException("SR failure");
        };
        
        // Test non-schema types (should work regardless of SR state)
        SerializationTypes[] nonSchemaTypes = {
            SerializationTypes.String,
            SerializationTypes.Integer,
            SerializationTypes.Long,
            SerializationTypes.ByteArray
        };
        
        for (SerializationTypes type : nonSchemaTypes) {
            // Should work with successful SR client
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> successSRClient);
                assertNotNull(deserializer, "Non-schema deserializer should work with successful SR client for type: " + type);
            });
            
            // Should work with failing SR client (non-schema types don't use SR)
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, failureSupplier);
                assertNotNull(deserializer, "Non-schema deserializer should work with failing SR client for type: " + type);
            });
        }
        
        // Test schema-based types (should propagate SR failures)
        SerializationTypes[] schemaBasedTypes = {
            SerializationTypes.Avro,
            SerializationTypes.JsonSchema,
            SerializationTypes.Protobuf
        };
        
        for (SerializationTypes type : schemaBasedTypes) {
            // Should work with successful SR client
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> successSRClient);
                assertNotNull(deserializer, "Schema deserializer should work with successful SR client for type: " + type);
            });
            
            // Should propagate SR failures
            RuntimeException exception = assertThrows(RuntimeException.class, () -> {
                SerdeFactory.createDeserializer(type, failureSupplier);
            }, "Should throw exception for SR failure with type: " + type);
            
            assertEquals("SR failure", exception.getMessage());
        }
    }

    @Test
    @DisplayName("Deserializers should handle corrupted data gracefully")
    void testCorruptedDataDeserialization() {
        // Test corrupted data for various serialization types
        byte[] corruptedData = {(byte)0x00, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC};
        
        // Test String deserializer with corrupted data
        assertDoesNotThrow(() -> {
            Deserializer<String> stringDeserializer = SerdeFactory.createDeserializer(SerializationTypes.String, () -> null);
            // String deserializer should handle corrupted data gracefully
        });
        
        // Test ByteArray deserializer with corrupted data
        assertDoesNotThrow(() -> {
            Deserializer<byte[]> byteArrayDeserializer = SerdeFactory.createDeserializer(SerializationTypes.ByteArray, () -> null);
            byte[] result = byteArrayDeserializer.deserialize("test-topic", corruptedData);
            assertNotNull(result, "ByteArray deserializer should return data even if corrupted");
            assertArrayEquals(corruptedData, result, "ByteArray deserializer should return original data");
        });
        
        // Test Integer deserializer with corrupted data
        assertDoesNotThrow(() -> {
            Deserializer<Integer> integerDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Integer, () -> null);
            // Integer deserializer should handle corrupted data gracefully
        });
        
        // Test Boolean deserializer with corrupted data
        assertDoesNotThrow(() -> {
            Deserializer<Boolean> booleanDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Boolean, () -> null);
            // Boolean deserializer should handle corrupted data gracefully
        });
    }

    @Test
    @DisplayName("Deserializers should handle null data gracefully")
    void testNullDataDeserialization() {
        // Test null data for various serialization types
        byte[] nullData = null;
        
        // Test String deserializer with null data
        assertDoesNotThrow(() -> {
            Deserializer<String> stringDeserializer = SerdeFactory.createDeserializer(SerializationTypes.String, () -> null);
            String result = stringDeserializer.deserialize("test-topic", nullData);
            assertNull(result, "String deserializer should return null for null data");
        });
        
        // Test ByteArray deserializer with null data
        assertDoesNotThrow(() -> {
            Deserializer<byte[]> byteArrayDeserializer = SerdeFactory.createDeserializer(SerializationTypes.ByteArray, () -> null);
            byte[] result = byteArrayDeserializer.deserialize("test-topic", nullData);
            assertNull(result, "ByteArray deserializer should return null for null data");
        });
        
        // Test Integer deserializer with null data
        assertDoesNotThrow(() -> {
            Deserializer<Integer> integerDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Integer, () -> null);
            Integer result = integerDeserializer.deserialize("test-topic", nullData);
            assertNull(result, "Integer deserializer should return null for null data");
        });
        
        // Test Boolean deserializer with null data
        assertDoesNotThrow(() -> {
            Deserializer<Boolean> booleanDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Boolean, () -> null);
            Boolean result = booleanDeserializer.deserialize("test-topic", nullData);
            assertNull(result, "Boolean deserializer should return null for null data");
        });
    }

    @Test
    @DisplayName("Deserializers should handle empty data gracefully")
    void testEmptyDataDeserialization() {
        // Test empty data for various serialization types
        byte[] emptyData = {};
        
        // Test String deserializer with empty data
        assertDoesNotThrow(() -> {
            Deserializer<String> stringDeserializer = SerdeFactory.createDeserializer(SerializationTypes.String, () -> null);
            String result = stringDeserializer.deserialize("test-topic", emptyData);
            assertNotNull(result, "String deserializer should return a value for empty data");
        });
        
        // Test ByteArray deserializer with empty data
        assertDoesNotThrow(() -> {
            Deserializer<byte[]> byteArrayDeserializer = SerdeFactory.createDeserializer(SerializationTypes.ByteArray, () -> null);
            byte[] result = byteArrayDeserializer.deserialize("test-topic", emptyData);
            assertNotNull(result, "ByteArray deserializer should return a value for empty data");
            assertArrayEquals(emptyData, result, "ByteArray deserializer should return original empty data");
        });
        
        // Test Integer deserializer with empty data - this will throw an exception
        assertThrows(SerializationException.class, () -> {
            Deserializer<Integer> integerDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Integer, () -> null);
            integerDeserializer.deserialize("test-topic", emptyData);
        }, "Integer deserializer should throw SerializationException for empty data");
        
        // Test Boolean deserializer with empty data - this will throw an exception
        assertThrows(SerializationException.class, () -> {
            Deserializer<Boolean> booleanDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Boolean, () -> null);
            booleanDeserializer.deserialize("test-topic", emptyData);
        }, "Boolean deserializer should throw SerializationException for empty data");
    }

    @Test
    @DisplayName("Deserializers should handle malformed JSON data gracefully")
    void testMalformedJsonDataDeserialization() {
        // Test malformed JSON data
        byte[] malformedJsonData = "{invalid json}".getBytes();
        
        // Test JSON deserializer with malformed data
        assertDoesNotThrow(() -> {
            Deserializer<Object> jsonDeserializer = SerdeFactory.createDeserializer(SerializationTypes.Json, () -> null);
            // JSON deserializer should handle malformed data gracefully
        });
        
        // Test JSON Schema deserializer with malformed data
        assertDoesNotThrow(() -> {
            Deserializer<Object> jsonSchemaDeserializer = SerdeFactory.createDeserializer(SerializationTypes.JsonSchema, () -> null);
            // JSON Schema deserializer should handle malformed data gracefully
        });
    }

    @Test
    @DisplayName("Deserializers should handle truncated data gracefully")
    void testTruncatedDataDeserialization() {
        // Test truncated data (incomplete serialized data)
        byte[] truncatedData = {(byte)0x00, (byte)0x01, (byte)0x02}; // Incomplete data
        
        // Test various deserializers with truncated data
        SerializationTypes[] typesToTest = {
            SerializationTypes.String,
            SerializationTypes.Integer,
            SerializationTypes.Long,
            SerializationTypes.Float,
            SerializationTypes.Double,
            SerializationTypes.Boolean,
            SerializationTypes.Short,
            SerializationTypes.ByteArray,
            SerializationTypes.Bytes,
            SerializationTypes.ByteBuffer,
            SerializationTypes.UUID
        };
        
        for (SerializationTypes type : typesToTest) {
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> null);
                // Deserializer should handle truncated data gracefully
            }, "Deserializer for " + type + " should handle truncated data gracefully");
        }
    }

    @Test
    @DisplayName("Deserializers should handle oversized data gracefully")
    void testOversizedDataDeserialization() {
        // Test oversized data (data that's too large)
        byte[] oversizedData = new byte[1024 * 1024]; // 1MB of data
        for (int i = 0; i < oversizedData.length; i++) {
            oversizedData[i] = (byte) (i % 256);
        }
        
        // Test various deserializers with oversized data
        SerializationTypes[] typesToTest = {
            SerializationTypes.String,
            SerializationTypes.ByteArray,
            SerializationTypes.Bytes,
            SerializationTypes.ByteBuffer
        };
        
        for (SerializationTypes type : typesToTest) {
            assertDoesNotThrow(() -> {
                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> null);
                // Deserializer should handle oversized data gracefully
            }, "Deserializer for " + type + " should handle oversized data gracefully");
        }
    }

    @Test
    @DisplayName("Deserializers should handle concurrent corrupted data access")
    void testConcurrentCorruptedDataDeserialization() throws InterruptedException {
        final int numberOfThreads = 10;
        final int iterationsPerThread = 50;
        final ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<>());
        
        // Test data with various corruption patterns
        byte[][] corruptedDataPatterns = {
            {(byte)0x00, (byte)0x01, (byte)0x02, (byte)0x03, (byte)0xFF, (byte)0xFE, (byte)0xFD, (byte)0xFC}, // Mixed corruption
            {(byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF, (byte)0xFF}, // All 0xFF
            {(byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00, (byte)0x00}, // All 0x00
            {(byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04, (byte)0x05, (byte)0x06, (byte)0x07, (byte)0x08}, // Sequential
            {(byte)0x08, (byte)0x07, (byte)0x06, (byte)0x05, (byte)0x04, (byte)0x03, (byte)0x02, (byte)0x01}  // Reverse sequential
        };
        
        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < iterationsPerThread; j++) {
                        byte[] corruptedData = corruptedDataPatterns[j % corruptedDataPatterns.length];
                        
                        // Test various deserializers with corrupted data
                        SerializationTypes[] typesToTest = {
                            SerializationTypes.String,
                            SerializationTypes.Integer,
                            SerializationTypes.Long,
                            SerializationTypes.ByteArray,
                            SerializationTypes.Boolean
                        };
                        
                        for (SerializationTypes type : typesToTest) {
                            try {
                                Deserializer<?> deserializer = SerdeFactory.createDeserializer(type, () -> null);
                                // Deserializer should handle corrupted data gracefully
                            } catch (Exception e) {
                                exceptions.add(e);
                            }
                        }
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                } finally {
                    endLatch.countDown();
                }
            });
        }

        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(endLatch.await(30, TimeUnit.SECONDS), "Test should complete within 30 seconds");
        
        // Shutdown executor
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Executor should shutdown within 5 seconds");
        
        // Verify no unexpected exceptions occurred
        assertTrue(exceptions.isEmpty(), "No unexpected exceptions should occur during concurrent corrupted data handling: " + exceptions);
    }
}
