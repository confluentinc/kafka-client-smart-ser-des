/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package confluent.client.common;

import com.google.protobuf.Message;
import confluent.client.common.schema.SchemaRegistryUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * This enum is used to determine the serialization type of the message.
 */
@Slf4j
public enum SerializationTypes {
    String,
    Bytes,
    ByteArray,
    ByteBuffer,
    Short,
    Integer,
    Long,
    Float,
    Double,
    UUID,
    Protobuf,
    Avro,
    JsonSchema,
    Json;

    public static final String VALUE_HEADER_KEY = "value.serialization.type";
    public static final String KEY_HEADER_KEY = "key.serialization.type";

    /**
     * Set the serialization type in the headers.
     *
     * @param headers The headers
     */
    public void toHeaders(final Headers headers, boolean isKey) {
        headers.add((isKey) ? KEY_HEADER_KEY : VALUE_HEADER_KEY, this.name().getBytes());
    }

    /**
     * This method is used to determine the serialization type from the headers.
     *
     * @param headers The headers
     * @return The serialization type
     */
    public static SerializationTypes fromHeaders(final Headers headers, boolean isKey) {
        if (headers == null) {
            return null;
        }

        final Header header = headers.lastHeader((isKey) ? KEY_HEADER_KEY : VALUE_HEADER_KEY);
        return (header != null)
                ? SerializationTypes.valueOf(new String(header.value()))
                : null;
    }

    /**
     * This method is used to determine the serialization type from a string.
     *
     * @param value The string value
     * @return The serialization type
     */
    public static SerializationTypes fromString(String value) {
        for (SerializationTypes type : SerializationTypes.values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }

        return Avro;
    }

    /**
     * This method is used to determine the serialization type of the message.
     *
     * @param clientSupplier Supplier of the Schema Registry Client
     * @param bytes          The message bytes
     * @return The schema type
     * @throws RestClientException If the Schema Registry Client fails
     * @throws IOException         If the Schema Registry Client fails
     */
    public static SerializationTypes fromSchema(Supplier<SchemaRegistryClient> clientSupplier, final byte[] bytes) throws RestClientException, IOException {
        final String schemaType = SchemaRegistryUtils.getSchemaType(clientSupplier, bytes);
        if (StringUtils.isEmpty(schemaType)) {
            return null;
        }

        switch (schemaType) {
            case "AVRO":
                log.info("Schema type is AVRO");
                return Avro;
            case "JSON":
                log.info("Schema type is JSON");
                return JsonSchema;
            case "PROTOBUF":
                log.info("Schema type is PROTOBUF");
                return Protobuf;
            default:
                return null;
        }
    }

    /**
     * This method is used to determine the serialization type of the message.
     *
     * @param clientSupplier Supplier of the Schema Registry Client
     * @param bytes          The message bytes
     * @return The serialization type
     * @throws RestClientException If the Schema Registry Client fails
     * @throws IOException         If the Schema Registry Client fails
     */
    public static SerializationTypes fromBytes(Supplier<SchemaRegistryClient> clientSupplier, final byte[] bytes) throws RestClientException, IOException {
        if (bytes.length == 1) {
            return SerializationTypes.ByteArray;
        } else if (bytes.length == 2) {
            return SerializationTypes.Short;
        } else if (bytes.length == 4) {
            return SerializationTypes.Integer;
        } else if (bytes.length == 8) {
            return SerializationTypes.Long;
        } else if (bytes.length == 0) {
            // edge case when passing in the empty string ""
            return SerializationTypes.String;
        }

        SerializationTypes serializationType = SerializationTypes.fromSchema(clientSupplier, bytes);
        if (serializationType != null) {
            return serializationType;
        }
        if ((bytes[0] == '{' && bytes[bytes.length - 1] == '}') ||
            (bytes[0] == '[' && bytes[bytes.length - 1] == ']')) {
            // Maybe a JSON ?
            return SerializationTypes.Json;
        }

        return SerializationTypes.String;
    }


    public static SerializationTypes fromClass(Class<?> tClass) {
        if (tClass.isAssignableFrom(byte[].class)) {
            return ByteArray;
        } else if (tClass.isAssignableFrom(String.class)) {
            return String;
        } else if (tClass.isAssignableFrom(Bytes.class)) {
            return Bytes;
        } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
            return ByteBuffer;
        } else if (tClass.isAssignableFrom(Float.class)) {
            return Float;
        } else if (tClass.isAssignableFrom(Double.class)) {
            return Double;
        } else if (tClass.isAssignableFrom(Integer.class)) {
            return Integer;
        } else if (tClass.isAssignableFrom(Long.class)) {
            return Long;
        } else if (tClass.isAssignableFrom(Short.class)) {
            return Short;
        } else if (tClass.isAssignableFrom(UUID.class)) {
            return UUID;
        } else if (tClass.isAssignableFrom(IndexedRecord.class)) {
            return Avro;
        } else if (tClass.isAssignableFrom(Message.class)) {
            return Protobuf;
        }

        return JsonSchema;
    }


    /**
     * This method is used to determine the serialization type of the message.
     *
     * @param data         The message data
     * @param srConfigured If the Schema Registry is configured
     * @return The serialization type
     */
    public static SerializationTypes fromClass(Object data, boolean srConfigured) {
        if (data instanceof String) {
            return SerializationTypes.String;
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
}
