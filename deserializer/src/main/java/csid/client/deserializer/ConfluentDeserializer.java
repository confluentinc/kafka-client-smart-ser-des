/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.deserializer;

import csid.client.SerializationTypes;
import csid.client.deserializer.exception.ConfluentDeserializerException;
import csid.client.schema.SchemaRegistryUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * KafkaSmartDeserializer is a wrapper around the Confluent Kafka Deserializers.
 *
 * @param <T> The type of the deserialized object.
 */
@Slf4j
public class ConfluentDeserializer<T> implements Deserializer<T> {
    private final Class<T> tClass;
    private Deserializer<?> inner;
    private final Map<String, ?> configuration;
    private final boolean isKey;
    private SchemaRegistryClient schemaRegistryClient;

    public ConfluentDeserializer(Properties configs, boolean isKey, Class<T> tClass) {
        this.tClass = tClass;
        configuration = configs
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        Map.Entry::getValue
                ));
        this.isKey = isKey;
    }

    public ConfluentDeserializer(Properties configs, boolean isKey, Class<T> tClass, SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.tClass = tClass;
        configuration = configs
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().toString(),
                        Map.Entry::getValue
                ));
        this.isKey = isKey;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String s, byte[] bytes) {
        if (inner == null) {
            try {
                init(bytes, null);
            } catch (RestClientException | IOException e) {
                throw new ConfluentDeserializerException("Error during inner deserializer initialization.", e);
            }
        }

        return (T) inner.deserialize(s, bytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        if (inner == null) {
            try {
                init(bytes, headers);
            } catch (RestClientException | IOException e) {
                throw new ConfluentDeserializerException("Error during inner deserializer initialization.", e);
            }
        }

        return (T) inner.deserialize(topic, headers, bytes);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
            inner = null;
        }
    }

    /**
     * Initialize the inner deserializer.
     *
     * @param bytes The bytes to deserialize.
     * @throws IOException         If an error occurs while retrieving the schema.
     * @throws RestClientException If an error occurs while retrieving the schema.
     */
    @Synchronized
    private void init(byte[] bytes, final Headers headers) throws RestClientException, IOException {
        if (inner != null) {
            return;
        }

        SerializationTypes serializationType = null;
        if (headers != null) {
            serializationType = SerializationTypes.fromHeaders(headers);
        }

        if (serializationType == null) {
            log.info("No serialization type found in headers. Trying to get it from the schema.");

            if (tClass.isAssignableFrom(byte[].class)) {
                log.info("byte[] deserializer");
                serializationType = SerializationTypes.ByteArray;
            } else if (tClass.isAssignableFrom(String.class)) {
                log.info("String deserializer");
                serializationType = SerializationTypes.String;
            } else if (tClass.isAssignableFrom(Bytes.class)) {
                log.info("Bytes deserializer");
                serializationType = SerializationTypes.Bytes;
            } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
                log.info("ByteBuffer deserializer");
                serializationType = SerializationTypes.ByteBuffer;
            } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
                log.info("ByteBuffer deserializer");
                serializationType = SerializationTypes.ByteBuffer;
            } else if (tClass.isAssignableFrom(Float.class)) {
                log.info("Float deserializer");
                serializationType = SerializationTypes.Float;
            } else if (tClass.isAssignableFrom(Double.class)) {
                log.info("Double deserializer");
                serializationType = SerializationTypes.Double;
            } else if (tClass.isAssignableFrom(Integer.class)) {
                log.info("Integer deserializer");
                serializationType = SerializationTypes.Integer;
            } else if (tClass.isAssignableFrom(Long.class)) {
                log.info("Long deserializer");
                serializationType = SerializationTypes.Long;
            } else if (tClass.isAssignableFrom(Short.class)) {
                log.info("Short deserializer");
                serializationType = SerializationTypes.Short;
            } else if (tClass.isAssignableFrom(UUID.class)) {
                log.info("UUID deserializer");
                serializationType = SerializationTypes.UUID;
            } else {
                // Get schema type
                serializationType = SerializationTypes.fromSchema(() -> {
                    if (schemaRegistryClient == null) {
                        schemaRegistryClient = getSchemaRegistryClient();
                    }
                    return schemaRegistryClient;
                }, bytes);
                if (serializationType == null) {
                    if ((bytes[0] == '{' && bytes[bytes.length - 1] == '}') ||
                            (bytes[0] == '[' && bytes[bytes.length - 1] == ']')) {
                        // Maybe a JSON ?
                        log.info("Json deserializer");
                        serializationType = SerializationTypes.Json;
                    } else {
                        log.info("ByteArray deserializer");
                        serializationType = SerializationTypes.ByteArray;
                    }
                }
            }
        }


        inner = getDeserializer(serializationType);
        inner.configure(configuration, isKey);
    }

    private Deserializer<?> getDeserializer(SerializationTypes serializationType) {
        switch (serializationType) {
            case Avro:
                return new KafkaAvroDeserializer(schemaRegistryClient);
            case Json:
                return new KafkaJsonDeserializer<>();
            case JsonSchema:
                return new KafkaJsonSchemaDeserializer<>(schemaRegistryClient);
            case Protobuf:
                return new KafkaProtobufDeserializer<>(schemaRegistryClient);
            case Long:
                return new LongDeserializer();
            case Integer:
                return new IntegerDeserializer();
            case Float:
                return new FloatDeserializer();
            case Double:
                return new DoubleDeserializer();
            case Short:
                return new ShortDeserializer();
            case UUID:
                return new UUIDDeserializer();
            case String:
                return new StringDeserializer();
            case Bytes:
                return new BytesDeserializer();
            case ByteBuffer:
                return new ByteBufferDeserializer();
            case ByteArray:
            default:
                return new ByteArrayDeserializer();
        }
    }

    /**
     * Get the schema registry client.
     *
     * @return The schema registry client.
     */
    @Synchronized
    private SchemaRegistryClient getSchemaRegistryClient() {
        if (schemaRegistryClient == null) {
            schemaRegistryClient = SchemaRegistryUtils.getSchemaRegistryClient(configuration);
        }

        return schemaRegistryClient;
    }
}
