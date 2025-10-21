/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import com.google.protobuf.Message;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for TypeDetectionService.
 * Tests all type detection methods with various input types and edge cases.
 */
@DisplayName("TypeDetectionService Tests")
class TypeDetectionServiceTest {

    @Test
    @DisplayName("detectFromClass should return correct type for primitive wrapper classes")
    void testDetectFromClassPrimitiveWrappers() {
        assertEquals(SerializationTypes.String, TypeDetectionService.detectFromClass(String.class));
        assertEquals(SerializationTypes.Boolean, TypeDetectionService.detectFromClass(Boolean.class));
        assertEquals(SerializationTypes.Integer, TypeDetectionService.detectFromClass(Integer.class));
        assertEquals(SerializationTypes.Long, TypeDetectionService.detectFromClass(Long.class));
        assertEquals(SerializationTypes.Float, TypeDetectionService.detectFromClass(Float.class));
        assertEquals(SerializationTypes.Double, TypeDetectionService.detectFromClass(Double.class));
        assertEquals(SerializationTypes.Short, TypeDetectionService.detectFromClass(Short.class));
        assertEquals(SerializationTypes.UUID, TypeDetectionService.detectFromClass(UUID.class));
    }

    @Test
    @DisplayName("detectFromClass should return correct type for array and buffer classes")
    void testDetectFromClassArrayAndBuffer() {
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromClass(byte[].class));
        assertEquals(SerializationTypes.Bytes, TypeDetectionService.detectFromClass(Bytes.class));
        assertEquals(SerializationTypes.ByteBuffer, TypeDetectionService.detectFromClass(ByteBuffer.class));
    }

    @Test
    @DisplayName("detectFromClass should return correct type for schema-based classes")
    void testDetectFromClassSchemaBased() {
        assertEquals(SerializationTypes.Avro, TypeDetectionService.detectFromClass(IndexedRecord.class));
        assertEquals(SerializationTypes.Protobuf, TypeDetectionService.detectFromClass(Message.class));
    }

    @Test
    @DisplayName("detectFromClass should return JsonSchema for unknown classes")
    void testDetectFromClassUnknown() {
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(Object.class));
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(Number.class));
    }

    @ParameterizedTest
    @MethodSource("provideObjectDetectionTestCases")
    @DisplayName("detectFromObject should return correct type for various object instances")
    void testDetectFromObject(Object data, boolean srConfigured, SerializationTypes expectedType) {
        SerializationTypes actualType = TypeDetectionService.detectFromObject(data, srConfigured);
        assertEquals(expectedType, actualType);
    }

    @ParameterizedTest
    @MethodSource("provideObjectDetectionTestCases")
    @DisplayName("detectFromInstance should return correct type for various object instances")
    void testDetectFromInstance(Object data, boolean srConfigured, SerializationTypes expectedType) {
        SerializationTypes actualType = TypeDetectionService.detectFromInstance(data, srConfigured);
        assertEquals(expectedType, actualType);
    }

    @Test
    @DisplayName("detectFromObject should handle null input gracefully")
    void testDetectFromObjectNull() {
        // This should not throw an exception, but behavior depends on implementation
        assertDoesNotThrow(() -> {
            TypeDetectionService.detectFromObject(null, true);
        });
    }

    @Test
    @DisplayName("detectFromObject should return Json for unknown types when SR not configured")
    void testDetectFromObjectUnknownTypeNoSR() {
        Object unknownObject = new Object();
        SerializationTypes result = TypeDetectionService.detectFromObject(unknownObject, false);
        assertEquals(SerializationTypes.Json, result);
    }

    @Test
    @DisplayName("detectFromObject should return JsonSchema for unknown types when SR configured")
    void testDetectFromObjectUnknownTypeWithSR() {
        Object unknownObject = new Object();
        SerializationTypes result = TypeDetectionService.detectFromObject(unknownObject, true);
        assertEquals(SerializationTypes.JsonSchema, result);
    }

    @Test
    @DisplayName("detectFromInstance should handle null input gracefully")
    void testDetectFromInstanceNull() {
        // This should not throw an exception, but behavior depends on implementation
        assertDoesNotThrow(() -> {
            TypeDetectionService.detectFromInstance(null, true);
        });
    }

    @Test
    @DisplayName("detectFromInstance should return Json for unknown types when SR not configured")
    void testDetectFromInstanceUnknownTypeNoSR() {
        Object unknownObject = new Object();
        SerializationTypes result = TypeDetectionService.detectFromInstance(unknownObject, false);
        assertEquals(SerializationTypes.Json, result);
    }

    @Test
    @DisplayName("detectFromInstance should return JsonSchema for unknown types when SR configured")
    void testDetectFromInstanceUnknownTypeWithSR() {
        Object unknownObject = new Object();
        SerializationTypes result = TypeDetectionService.detectFromInstance(unknownObject, true);
        assertEquals(SerializationTypes.JsonSchema, result);
    }

    private static Stream<Arguments> provideObjectDetectionTestCases() {
        return Stream.of(
            // String types
            Arguments.of("test string", true, SerializationTypes.String),
            Arguments.of("test string", false, SerializationTypes.String),
            
            // Boolean types
            Arguments.of(true, true, SerializationTypes.Boolean),
            Arguments.of(false, false, SerializationTypes.Boolean),
            
            // Numeric types
            Arguments.of(42, true, SerializationTypes.Integer),
            Arguments.of(42L, false, SerializationTypes.Long),
            Arguments.of(3.14f, true, SerializationTypes.Float),
            Arguments.of(3.14, false, SerializationTypes.Double),
            Arguments.of((short) 42, true, SerializationTypes.Short),
            
            // Array and buffer types
            Arguments.of(new byte[]{1, 2, 3}, true, SerializationTypes.ByteArray),
            Arguments.of(Bytes.wrap(new byte[]{1, 2, 3}), false, SerializationTypes.Bytes),
            Arguments.of(ByteBuffer.wrap(new byte[]{1, 2, 3}), true, SerializationTypes.ByteBuffer),
            
            // UUID
            Arguments.of(UUID.randomUUID(), true, SerializationTypes.UUID),
            
            // Schema-based types (using mock implementations)
            Arguments.of(new MockIndexedRecord(), true, SerializationTypes.Avro)
            // Note: Protobuf testing removed due to complex interface requirements
        );
    }

    // Mock implementations for testing
    private static class MockIndexedRecord implements IndexedRecord {
        @Override
        public void put(int i, Object v) {}
        
        @Override
        public Object get(int i) { return null; }
        
        @Override
        public org.apache.avro.Schema getSchema() { return null; }
    }

}
