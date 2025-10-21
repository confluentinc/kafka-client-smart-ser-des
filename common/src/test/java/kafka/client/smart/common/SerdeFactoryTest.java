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
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

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
}
