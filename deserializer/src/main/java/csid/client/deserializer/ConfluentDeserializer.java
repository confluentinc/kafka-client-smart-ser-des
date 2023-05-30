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
                serializationType = SerializationTypes.ByteArray;
            } else {
                serializationType = SerializationTypes.ByteArray;
                // Create inner deserializer based on the type of the object to deserialize.
                if (tClass.isAssignableFrom(String.class)) {
                    serializationType = SerializationTypes.String;
                } else if (tClass.isAssignableFrom(Bytes.class)) {
                    serializationType = SerializationTypes.Bytes;
                } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
                    serializationType = SerializationTypes.ByteBuffer;
                } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
                    serializationType = SerializationTypes.ByteBuffer;
                } else if (tClass.isAssignableFrom(Float.class)) {
                    serializationType = SerializationTypes.Float;
                } else if (tClass.isAssignableFrom(Double.class)) {
                    serializationType = SerializationTypes.Double;
                } else if (tClass.isAssignableFrom(Integer.class)) {
                    serializationType = SerializationTypes.Integer;
                } else if (tClass.isAssignableFrom(Long.class)) {
                    serializationType = SerializationTypes.Long;
                } else if (tClass.isAssignableFrom(Short.class)) {
                    serializationType = SerializationTypes.Short;
                } else if (tClass.isAssignableFrom(UUID.class)) {
                    serializationType = SerializationTypes.UUID;
                } else {
                    // Get schema ID
                    final Integer schemaID = getSchemaID(bytes);
                    if (schemaID != null) {
                        if (schemaRegistryClient == null) {
                            schemaRegistryClient = SchemaRegistryUtils.getSchemaRegistryClient(configuration);
                        }

                        // Schema base payload.
                        ParsedSchema parsedSchema = this.schemaRegistryClient.getSchemaById(schemaID);
                        switch (parsedSchema.schemaType()) {
                            case "AVRO":
                                serializationType = SerializationTypes.Avro;
                                break;
                            case "JSON":
                                serializationType = SerializationTypes.JsonSchema;
                                break;
                            case "PROTOBUF":
                                serializationType = SerializationTypes.Protobuf;
                                break;
                        }
                    } else if ((bytes[0] == '{' && bytes[bytes.length - 1] == '}') ||
                            (bytes[0] == '[' && bytes[bytes.length - 1] == ']')) {
                        // Maybe a JSON ?
                        serializationType = SerializationTypes.Json;
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
     * Get the schema ID from the payload.
     *
     * @param bytes The bytes to deserialize.
     * @return The schema ID.
     */
    private Integer getSchemaID(byte[] bytes) {
        if (bytes[0] != 0) {
            return null;
        } else {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 1, 4);
            return byteBuffer.getInt();
        }
    }
}
