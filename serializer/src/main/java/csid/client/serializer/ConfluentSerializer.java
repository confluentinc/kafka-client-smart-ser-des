/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package csid.client.serializer;

import csid.client.common.Lazy;
import csid.client.common.SerializationTypes;
import csid.client.common.caching.ConfluentSerdeCache;
import csid.client.common.schema.SchemaRegistryUtils;
import csid.client.serializer.internal.ConfluentSerializerInternal;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

import java.util.Map;


/**
 * A serializer that can serialize objects of any type using Confluent's serializers.
 */
@Slf4j
public class ConfluentSerializer implements Serializer<Object> {

    private ConfluentSerdeCache<Serializer<Object>> cacheInstance;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Lazy<SchemaRegistryClient> srSupplier = new Lazy<>(() -> SchemaRegistryUtils.getSchemaRegistryClient(configs));
        cacheInstance = new ConfluentSerdeCache<>(this::getSerializer, srSupplier, configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return cacheInstance.getOrCreate(data).serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return cacheInstance.getOrCreate(data).serialize(topic, headers, data);
    }

    @Override
    public void close() {
        cacheInstance.close();
    }

    private Serializer<Object> getSerializer(SerializationTypes serializationTypes, Map<String, ?> cfg, boolean key) {
        final ConfluentSerializerInternal<Object> serializer = new ConfluentSerializerInternal<>(serializationTypes);
        serializer.configure(cfg, key);
        return serializer;
    }
}