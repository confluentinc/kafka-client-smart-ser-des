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
import java.util.function.Supplier;
import java.util.stream.Stream;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

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
    @DisplayName("detectFromClass should handle primitive types correctly")
    void testDetectFromClassPrimitiveTypes() {
        // Test that primitive types return the same SerializationTypes as their wrapper classes
        assertEquals(SerializationTypes.Boolean, TypeDetectionService.detectFromClass(boolean.class));
        assertEquals(SerializationTypes.Integer, TypeDetectionService.detectFromClass(int.class));
        assertEquals(SerializationTypes.Long, TypeDetectionService.detectFromClass(long.class));
        assertEquals(SerializationTypes.Float, TypeDetectionService.detectFromClass(float.class));
        assertEquals(SerializationTypes.Double, TypeDetectionService.detectFromClass(double.class));
        assertEquals(SerializationTypes.Short, TypeDetectionService.detectFromClass(short.class));
        // Note: byte.class and char.class are not supported in the current SerializationTypes enum
        // They would fall back to JsonSchema as the default
    }

    @Test
    @DisplayName("detectFromClass should return same type for primitive and wrapper classes")
    void testDetectFromClassPrimitiveVsWrapper() {
        // Verify primitive and wrapper classes return the same SerializationTypes
        assertEquals(
            TypeDetectionService.detectFromClass(Boolean.class),
            TypeDetectionService.detectFromClass(boolean.class),
            "Boolean.class and boolean.class should return the same SerializationTypes"
        );
        assertEquals(
            TypeDetectionService.detectFromClass(Integer.class),
            TypeDetectionService.detectFromClass(int.class),
            "Integer.class and int.class should return the same SerializationTypes"
        );
        assertEquals(
            TypeDetectionService.detectFromClass(Long.class),
            TypeDetectionService.detectFromClass(long.class),
            "Long.class and long.class should return the same SerializationTypes"
        );
        assertEquals(
            TypeDetectionService.detectFromClass(Float.class),
            TypeDetectionService.detectFromClass(float.class),
            "Float.class and float.class should return the same SerializationTypes"
        );
        assertEquals(
            TypeDetectionService.detectFromClass(Double.class),
            TypeDetectionService.detectFromClass(double.class),
            "Double.class and double.class should return the same SerializationTypes"
        );
        assertEquals(
            TypeDetectionService.detectFromClass(Short.class),
            TypeDetectionService.detectFromClass(short.class),
            "Short.class and short.class should return the same SerializationTypes"
        );
        // Note: Byte.class/byte.class and Character.class/char.class are not supported
        // in the current SerializationTypes enum, so they are not tested here
    }

    @Test
    @DisplayName("detectFromClass should handle unsupported primitive types gracefully")
    void testDetectFromClassUnsupportedPrimitives() {
        // Test that unsupported primitive types fall back to default behavior
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(byte.class));
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(char.class));
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(Byte.class));
        assertEquals(SerializationTypes.JsonSchema, TypeDetectionService.detectFromClass(Character.class));
    }

    @Test
    @DisplayName("detectFromObject should handle empty byte arrays correctly")
    void testDetectFromObjectEmptyByteArray() {
        // Test empty byte array - should be detected as ByteArray
        byte[] emptyArray = new byte[0];
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(emptyArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(emptyArray, true));
    }

    @Test
    @DisplayName("detectFromObject should handle single-byte arrays correctly")
    void testDetectFromObjectSingleByteArray() {
        // Test single-byte arrays - should be detected as ByteArray regardless of value
        byte[] singleByteArray = new byte[1];
        singleByteArray[0] = 0x00; // Boolean false
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, true));
        
        singleByteArray[0] = 0x01; // Boolean true
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, true));
        
        singleByteArray[0] = 0x42; // Some other value
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromObject(singleByteArray, true));
    }

    @Test
    @DisplayName("detectFromInstance should handle empty byte arrays correctly")
    void testDetectFromInstanceEmptyByteArray() {
        // Test empty byte array - should be detected as ByteArray
        byte[] emptyArray = new byte[0];
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(emptyArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(emptyArray, true));
    }

    @Test
    @DisplayName("detectFromInstance should handle single-byte arrays correctly")
    void testDetectFromInstanceSingleByteArray() {
        // Test single-byte arrays - should be detected as ByteArray regardless of value
        byte[] singleByteArray = new byte[1];
        singleByteArray[0] = 0x00; // Boolean false
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, true));
        
        singleByteArray[0] = 0x01; // Boolean true
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, true));
        
        singleByteArray[0] = 0x42; // Some other value
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, false));
        assertEquals(SerializationTypes.ByteArray, TypeDetectionService.detectFromInstance(singleByteArray, true));
    }

    @Test
    @DisplayName("SerializationTypes.fromBytes should handle empty vs single-byte arrays correctly")
    void testSerializationTypesFromBytesEmptyVsSingleByte() throws Exception {
        // Mock SchemaRegistryClient supplier that returns null (no schema found)
        Supplier<SchemaRegistryClient> mockSupplier = () -> null;
        
        // Test empty byte array - should return String (edge case for empty string "")
        byte[] emptyArray = new byte[0];
        assertEquals(SerializationTypes.String, SerializationTypes.fromBytes(mockSupplier, emptyArray));
        
        // Test single-byte array with 0x00 - should return Boolean (false)
        byte[] singleByteFalse = new byte[]{0x00};
        assertEquals(SerializationTypes.Boolean, SerializationTypes.fromBytes(mockSupplier, singleByteFalse));
        
        // Test single-byte array with 0x01 - should return Boolean (true)
        byte[] singleByteTrue = new byte[]{0x01};
        assertEquals(SerializationTypes.Boolean, SerializationTypes.fromBytes(mockSupplier, singleByteTrue));
        
        // Test single-byte array with other value - should return ByteArray
        byte[] singleByteOther = new byte[]{0x42};
        assertEquals(SerializationTypes.ByteArray, SerializationTypes.fromBytes(mockSupplier, singleByteOther));
        
        // Test two-byte array - should return Short
        byte[] twoByteArray = new byte[]{0x00, 0x01};
        assertEquals(SerializationTypes.Short, SerializationTypes.fromBytes(mockSupplier, twoByteArray));
        
        // Test four-byte array - should return Integer
        byte[] fourByteArray = new byte[]{0x00, 0x01, 0x02, 0x03};
        assertEquals(SerializationTypes.Integer, SerializationTypes.fromBytes(mockSupplier, fourByteArray));
        
        // Test eight-byte array - should return Long
        byte[] eightByteArray = new byte[]{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
        assertEquals(SerializationTypes.Long, SerializationTypes.fromBytes(mockSupplier, eightByteArray));
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
