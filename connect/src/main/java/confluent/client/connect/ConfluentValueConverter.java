/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package confluent.client.connect;

import confluent.client.common.SerializationTypes;
import confluent.client.common.caching.ConfluentSerdeCache;
import confluent.client.common.schema.SchemaRegistryUtils;
import confluent.client.connect.internal.ConfluentValueConverterInternal;
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
