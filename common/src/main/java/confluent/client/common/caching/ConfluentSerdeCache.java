/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package confluent.client.common.caching;

import confluent.client.common.SerializationTypes;
import confluent.client.common.exception.ConfluentSchemaException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * ConfluentSerdeCache is a wrapper around the Confluent serializers.
 *
 * @param <T> The type of object that is being lazily initialized.
 */
public class ConfluentSerdeCache<T> {

    /**
     * SerdeFactory is a factory for creating serializers.
     *
     * @param <T> The type of object that is being lazily initialized.
     */
    public interface SerdeFactory<T> {
        T create(SerializationTypes serializationTypes, Map<String, ?> configs, boolean isKey);
    }

    private final Map<SerializationTypes, T> cache = new ConcurrentHashMap<>();
    private final Map<String, Object> configs;
    private final boolean isKey;
    private final boolean srConfigured;
    private final SerdeFactory<T> factory;
    private final Supplier<SchemaRegistryClient> srSupplier;
    private final SerializationTypes defaultType;

    public ConfluentSerdeCache(SerdeFactory<T> factory, Supplier<SchemaRegistryClient> srSupplier, Map<String, ?> configs, boolean isKey, SerializationTypes defaultType) {
        this.factory = factory;
        this.srSupplier = srSupplier;
        this.configs = new HashMap<>(configs);
        this.isKey = isKey;
        this.srConfigured = configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        this.defaultType = defaultType;


        // If the user has not configured a subject name strategy, we will use the topic name strategy.
        if (isKey) {
            if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY)) {
                this.configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
            }
        } else if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY)) {
            this.configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        }
    }

    public ConfluentSerdeCache(SerdeFactory<T> factory, Supplier<SchemaRegistryClient> srSupplier, Map<String, ?> configs, boolean isKey) {
        this(factory, srSupplier, configs, isKey, null);
    }

    /**
     * Get the serde for the given serialization type.
     *
     * @param data    The data to be serialized.
     * @param headers The headers to be used for determining the serialization type.
     * @return The serde for the given serialization type.
     */
    public T getOrCreate(final byte[] data, final Headers headers) {
        SerializationTypes serializationType = SerializationTypes.fromHeaders(headers, isKey);
        if (serializationType == null) {
            try {
                if (defaultType != null) {
                    serializationType = defaultType;
                } else {
                    serializationType = SerializationTypes.fromBytes(srSupplier, data);
                }
            } catch (RestClientException | IOException e) {
                throw new ConfluentSchemaException("Could not determine the serialization type.", e);
            }
        }

        if (serializationType == null) {
            throw new SerializationException("Could not determine the serialization type.");
        }

        return getOrCreate(serializationType);
    }

    /**
     * Get the serde for the given serialization type.
     *
     * @param data The data to be serialized.
     * @return The serde for the given serialization type.
     */
    public T getOrCreate(Object data) {
        final SerializationTypes serializationType = SerializationTypes.fromClass(data, srConfigured);
        return (serializationType == null)
                ? getOrCreate(defaultType)
                : getOrCreate(serializationType);
    }

    /**
     * Get the serde for the given serialization type.
     *
     * @param data        The data to be serialized.
     * @param isPrimitive Whether the data is a primitive type.
     * @return The serde for the given serialization type.
     */
    public T getOrCreate(Object data, boolean isPrimitive) {
        final SerializationTypes serializationTypes = isPrimitive
                ? SerializationTypes.fromClass(data.getClass())
                : defaultType;
        return getOrCreate(serializationTypes);
    }

    /**
     * Close all of the cached serializers.
     */
    public void close() {
        cache.values().forEach(serde -> {
            if (serde instanceof Closeable) {
                try {
                    ((Closeable) serde).close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        });
    }

    /**
     * Get the serde from the cache or create a new one.
     *
     * @param serializationType The serialization type.
     * @return The serde for the given serialization type.
     */
    private T getOrCreate(SerializationTypes serializationType) {
        return cache.computeIfAbsent(serializationType, type -> factory.create(type, configs, isKey));
    }
}
