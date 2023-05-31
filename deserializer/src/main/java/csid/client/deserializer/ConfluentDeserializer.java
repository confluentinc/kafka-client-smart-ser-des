/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.deserializer;

import csid.client.common.SerializationTypes;
import csid.client.deserializer.exception.ConfluentDeserializerException;
import csid.client.common.schema.SchemaRegistryUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * KafkaSmartDeserializer is a wrapper around the Confluent Kafka Deserializers.
 *
 * @param <T> The type of the deserialized object.
 */
@Slf4j
public class ConfluentDeserializer<T> implements Deserializer<T> {
    private Class<T> tClass;
    private Deserializer<?> inner;
    private Map<String, ?> configuration;
    private boolean isKey;
    private SchemaRegistryClient schemaRegistryClient;

    public ConfluentDeserializer() {
    }

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
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);

        configuration = configs;
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

            serializationType = (tClass == null)
                    ? SerializationTypes.fromBytes(this::getSchemaRegistryClient, bytes)
                    : SerializationTypes.fromClass(this::getSchemaRegistryClient, tClass, bytes);

            log.info("Serialization type found: {}", serializationType);
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
