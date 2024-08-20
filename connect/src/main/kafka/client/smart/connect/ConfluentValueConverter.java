/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.connect;

import kafka.client.smart.common.SerializationTypes;
import kafka.client.smart.common.caching.ConfluentSerdeCache;
import kafka.client.smart.common.schema.SchemaRegistryUtils;
import kafka.client.smart.connect.internal.ConfluentValueConverterInternal;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * This class is a wrapper around the Confluent converters.
 */
@Slf4j
public class ConfluentValueConverter implements Converter {

    private ConfluentSerdeCache<Converter> cacheInstance;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final ConfluentValueConverterConfig config = new ConfluentValueConverterConfig(configs);

        final SerializationTypes serializationTypes = config.getType();

        cacheInstance = new ConfluentSerdeCache<>(
                this::getConverter,
                () -> SchemaRegistryUtils.getSchemaRegistryClient(configs),
                configs,
                isKey,
                serializationTypes);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return cacheInstance.getOrCreate(value, schema.type().isPrimitive())
                .fromConnectData(topic, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        return cacheInstance.getOrCreate(value, schema.type().isPrimitive())
                .fromConnectData(topic, headers, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return cacheInstance.getOrCreate(value, null).toConnectData(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return cacheInstance.getOrCreate(value, headers).toConnectData(topic, value);
    }

    private Converter getConverter(SerializationTypes serializationTypes, Map<String, ?> cfg, boolean isKey) {
        ConfluentValueConverterInternal converter = new ConfluentValueConverterInternal(serializationTypes, isKey);
        converter.configure(cfg, isKey);
        return converter;
    }
}
