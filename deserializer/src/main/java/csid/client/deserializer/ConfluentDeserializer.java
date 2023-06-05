/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.deserializer;

import csid.client.common.Lazy;
import csid.client.common.SerializationTypes;
import csid.client.common.caching.ConfluentSerdeCache;
import csid.client.common.schema.SchemaRegistryUtils;
import csid.client.deserializer.internal.ConfluentDeserializerInternal;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * KafkaSmartDeserializer is a wrapper around the Confluent Kafka Deserializers.
 */
@Slf4j
public class ConfluentDeserializer implements Deserializer<Object> {
    private ConfluentSerdeCache<Deserializer<Object>> cacheInstance;
    private final Class<?> tClass;
    private Lazy<SchemaRegistryClient> srSupplier;

    public ConfluentDeserializer() {
        tClass = null;
    }

    public ConfluentDeserializer(Class<?> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final boolean srConfigured = SchemaRegistryUtils.isSchemaRegistryConfigured(configs);
        srSupplier = new Lazy<>(() -> SchemaRegistryUtils.getSchemaRegistryClient(configs));

        SerializationTypes serializationTypes = (tClass != null)
                ? SerializationTypes.fromClass(tClass)
                : null;
        if (serializationTypes == SerializationTypes.JsonSchema && !srConfigured) {
            serializationTypes = SerializationTypes.Json;
        }
        cacheInstance = new ConfluentSerdeCache<>(
                this::getDeserializer, () -> SchemaRegistryUtils.getSchemaRegistryClient(configs),
                configs,
                isKey,
                serializationTypes);
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        return cacheInstance.get(bytes, null).deserialize(topic, bytes);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] bytes) {
        return cacheInstance.get(bytes, headers).deserialize(topic, headers, bytes);
    }

    @Override
    public void close() {
        cacheInstance.close();
    }

    private Deserializer<Object> getDeserializer(SerializationTypes serializationTypes, Map<String, ?> cfg, boolean isKey) {
        final ConfluentDeserializerInternal<Object> deserializer = new ConfluentDeserializerInternal<>(serializationTypes, srSupplier);
        deserializer.configure(cfg, isKey);
        return deserializer;
    }
}
