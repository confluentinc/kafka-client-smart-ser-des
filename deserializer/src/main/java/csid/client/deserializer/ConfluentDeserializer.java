/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.deserializer;

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
                init(bytes);
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
                init(bytes);
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
    private void init(byte[] bytes) throws RestClientException, IOException {
        if (inner != null) {
            return;
        }

        // Create inner deserializer based on the type of the object to deserialize.
        if (tClass.isAssignableFrom(String.class)) {
            inner = new StringDeserializer();
        } else if (tClass.isAssignableFrom(byte[].class)) {
            inner = new ByteArrayDeserializer();
        } else if (tClass.isAssignableFrom(ByteBuffer.class)) {
            inner = new ByteBufferDeserializer();
        } else if (tClass.isAssignableFrom(Float.class)) {
            inner = new FloatDeserializer();
        } else if (tClass.isAssignableFrom(Double.class)) {
            inner = new DoubleDeserializer();
        } else if (tClass.isAssignableFrom(Integer.class)) {
            inner = new IntegerDeserializer();
        } else if (tClass.isAssignableFrom(Long.class)) {
            inner = new LongDeserializer();
        } else if (tClass.isAssignableFrom(Short.class)) {
            inner = new ShortDeserializer();
        } else if (tClass.isAssignableFrom(Bytes.class)) {
            inner = new BytesDeserializer();
        } else if (tClass.isAssignableFrom(UUID.class)) {
            inner = new UUIDDeserializer();
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
                        inner = new KafkaAvroDeserializer(schemaRegistryClient);
                        break;
                    case "JSON":
                        inner = new KafkaJsonSchemaDeserializer<>(schemaRegistryClient);
                        break;
                    case "PROTOBUF":
                        inner = new KafkaProtobufDeserializer<>(schemaRegistryClient);
                        break;
                    default:
                        inner = new ByteArrayDeserializer();
                        break;
                }
            } else if ((bytes[0] == '{' && bytes[bytes.length - 1] == '}') ||
                    (bytes[0] == '[' && bytes[bytes.length - 1] == ']')) {
                // Maybe a JSON ?
                inner = new KafkaJsonDeserializer<>();
            } else {
                inner = new ByteArrayDeserializer();
            }
        }

        inner.configure(configuration, isKey);
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
