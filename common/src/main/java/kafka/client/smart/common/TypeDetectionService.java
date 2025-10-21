/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Centralized service for detecting serialization types from various sources.
 * This eliminates code duplication across SerializationTypes and ConfluentSerializerInternal.
 */
@Slf4j
public final class TypeDetectionService {

    private TypeDetectionService() {
        // Utility class
    }

    /**
     * Detect serialization type from a Class object.
     *
     * @param tClass The class to analyze
     * @return The corresponding SerializationTypes enum value
     */
    public static SerializationTypes detectFromClass(Class<?> tClass) {
        if (tClass.isAssignableFrom(byte[].class)) {
            return SerializationTypes.ByteArray;
        } else if (tClass.isAssignableFrom(Boolean.class)) {
            return SerializationTypes.Boolean;
        } else if (tClass.isAssignableFrom(String.class)) {
            return SerializationTypes.String;
        } else if (tClass.isAssignableFrom(Bytes.class)) {
            return SerializationTypes.Bytes;
        } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
            return SerializationTypes.ByteBuffer;
        } else if (tClass.isAssignableFrom(Float.class)) {
            return SerializationTypes.Float;
        } else if (tClass.isAssignableFrom(Double.class)) {
            return SerializationTypes.Double;
        } else if (tClass.isAssignableFrom(Integer.class)) {
            return SerializationTypes.Integer;
        } else if (tClass.isAssignableFrom(Long.class)) {
            return SerializationTypes.Long;
        } else if (tClass.isAssignableFrom(Short.class)) {
            return SerializationTypes.Short;
        } else if (tClass.isAssignableFrom(UUID.class)) {
            return SerializationTypes.UUID;
        } else if (tClass.isAssignableFrom(IndexedRecord.class)) {
            return SerializationTypes.Avro;
        } else if (tClass.isAssignableFrom(Message.class)) {
            return SerializationTypes.Protobuf;
        }

        return SerializationTypes.JsonSchema;
    }

    /**
     * Detect serialization type from an Object instance with Schema Registry configuration context.
     *
     * @param data The object to analyze
     * @param srConfigured Whether Schema Registry is configured
     * @return The corresponding SerializationTypes enum value
     */
    public static SerializationTypes detectFromObject(Object data, boolean srConfigured) {
        if (data instanceof String) {
            return SerializationTypes.String;
        } else if (data instanceof Boolean) {
            return SerializationTypes.Boolean;
        } else if (data instanceof byte[]) {
            return SerializationTypes.ByteArray;
        } else if (data instanceof Short) {
            return SerializationTypes.Short;
        } else if (data instanceof Integer) {
            return SerializationTypes.Integer;
        } else if (data instanceof Long) {
            return SerializationTypes.Long;
        } else if (data instanceof Float) {
            return SerializationTypes.Float;
        } else if (data instanceof Double) {
            return SerializationTypes.Double;
        } else if (data instanceof Bytes) {
            return SerializationTypes.Bytes;
        } else if (data instanceof UUID) {
            return SerializationTypes.UUID;
        } else if (data instanceof ByteBuffer) {
            return SerializationTypes.ByteBuffer;
        } else if (data instanceof Message) {
            return SerializationTypes.Protobuf;
        } else if (data instanceof IndexedRecord) {
            return SerializationTypes.Avro;
        } else if (srConfigured) {
            return SerializationTypes.JsonSchema;
        } else {
            return SerializationTypes.Json;
        }
    }

    /**
     * Detect serialization type from an Object instance (used in serializer initialization).
     * This method provides the same logic as the original init() method in ConfluentSerializerInternal.
     *
     * @param data The object to analyze
     * @param srConfigured Whether Schema Registry is configured
     * @return The corresponding SerializationTypes enum value
     */
    public static SerializationTypes detectFromInstance(Object data, boolean srConfigured) {
        if (data instanceof String) {
            return SerializationTypes.String;
        } else if (data instanceof Boolean) {
            return SerializationTypes.Boolean;
        } else if (data instanceof byte[]) {
            return SerializationTypes.ByteArray;
        } else if (data instanceof Short) {
            return SerializationTypes.Short;
        } else if (data instanceof Integer) {
            return SerializationTypes.Integer;
        } else if (data instanceof Long) {
            return SerializationTypes.Long;
        } else if (data instanceof Float) {
            return SerializationTypes.Float;
        } else if (data instanceof Double) {
            return SerializationTypes.Double;
        } else if (data instanceof Bytes) {
            return SerializationTypes.Bytes;
        } else if (data instanceof UUID) {
            return SerializationTypes.UUID;
        } else if (data instanceof ByteBuffer) {
            return SerializationTypes.ByteBuffer;
        } else if (data instanceof Message) {
            return SerializationTypes.Protobuf;
        } else if (data instanceof IndexedRecord) {
            return SerializationTypes.Avro;
        } else {
            // Check if SR was configured
            if (srConfigured) {
                return SerializationTypes.JsonSchema;
            } else {
                return SerializationTypes.Json;
            }
        }
    }
}
