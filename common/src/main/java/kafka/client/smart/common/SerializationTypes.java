/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import kafka.client.smart.common.schema.SchemaRegistryUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * This enum is used to determine the serialization type of the message.
 */
@Slf4j
public enum SerializationTypes {
    String,
    Boolean,
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
            // Check if the single byte is 0x00 or 0x01, indicating a Boolean value
            if (bytes[0] == 0x00 || bytes[0] == 0x01) {
                return SerializationTypes.Boolean;
            } else {
                return SerializationTypes.ByteArray;
            }
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
        return TypeDetectionService.detectFromClass(tClass);
    }


    /**
     * This method is used to determine the serialization type of the message.
     *
     * @param data         The message data
     * @param srConfigured If the Schema Registry is configured
     * @return The serialization type
     */
    public static SerializationTypes fromClass(Object data, boolean srConfigured) {
        return TypeDetectionService.detectFromObject(data, srConfigured);
    }
}
