package csid.client.deserializer.internal;

import csid.client.common.SerializationTypes;
import csid.client.common.schema.SchemaRegistryUtils;
import csid.client.deserializer.exception.ConfluentDeserializerException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to cache the deserializers.
 */
@Slf4j
public class ConfluentDeserializerCache implements Closeable {

    private final Map<SerializationTypes, Deserializer<Object>> cache = new ConcurrentHashMap<>();
    private SerializationTypes defaultType;
    private Map<String, Object> configs;
    private boolean isKey;
    private boolean srConfigured;
    private SchemaRegistryClient schemaRegistryClient;

    public ConfluentDeserializerCache() {
    }

    public Deserializer<Object> get(final byte[] data, final Headers headers) {
        SerializationTypes serializationType = SerializationTypes.fromHeaders(headers, isKey);
        if (serializationType == null) {
            try {
                if (defaultType != null) {
                    serializationType = defaultType;
                } else {
                    serializationType = SerializationTypes.fromBytes(this::getSchemaRegistryClient, data);
                }
            } catch (RestClientException | IOException e) {
                throw new ConfluentDeserializerException("Could not determine the serialization type.", e);
            }
        }

        if (serializationType == null) {
            throw new ConfluentDeserializerException("Could not determine the serialization type.");
        }

        return get(serializationType);
    }

    public void configure(Map<String, ?> configs, final boolean isKey, final Class<?> tClass) {
        this.configs = new HashMap<>(configs);
        this.srConfigured = configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);

        if (tClass != null) {
            this.defaultType = SerializationTypes.fromClass(tClass);
        }

        if (this.srConfigured) {
            // If the user has not configured a subject name strategy, we will use the topic name strategy.
            if (isKey) {
                if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY)) {
                    this.configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
                }
            } else if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY)) {
                this.configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
            }
        } else if (defaultType == SerializationTypes.JsonSchema) {
            defaultType = SerializationTypes.Json;
        }

        this.isKey = isKey;
    }

    /**
     * Get the serializer for the given serialization type.
     *
     * @param serializationType The serialization type.
     * @return The serializer.
     */
    private Deserializer<Object> get(SerializationTypes serializationType) {
        if (!cache.containsKey(serializationType)) {
            init(serializationType);
        }

        return cache.get(serializationType);
    }

    /**
     * This method is synchronized to avoid multiple threads to initialize the same serializer.
     *
     * @param serializationType The serialization type.
     */
    @Synchronized
    private void init(SerializationTypes serializationType) {
        if (cache.containsKey(serializationType)) {
            return;
        }

        Deserializer<Object> deserializer = new ConfluentDeserializerInternal<>(serializationType, getSchemaRegistryClient());
        deserializer.configure(configs, isKey);

        cache.put(serializationType, deserializer);
    }

    @Override
    @Synchronized
    public void close() {
        cache.forEach((k, v) -> v.close());
        cache.clear();
    }

    /**
     * Get the schema registry client.
     *
     * @return The schema registry client.
     */
    @Synchronized
    private SchemaRegistryClient getSchemaRegistryClient() {
        if (!srConfigured) {
            return null;
        }

        if (schemaRegistryClient == null) {
            schemaRegistryClient = SchemaRegistryUtils.getSchemaRegistryClient(configs);
        }

        return schemaRegistryClient;
    }
}
