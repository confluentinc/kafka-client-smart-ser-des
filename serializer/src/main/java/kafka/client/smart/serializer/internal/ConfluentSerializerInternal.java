/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.serializer.internal;

import kafka.client.smart.common.SerializationTypes;
import kafka.client.smart.common.TypeDetectionService;
import kafka.client.smart.common.SerdeFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

import java.util.Map;

/**
 * ConfluentSerializerInternal is a wrapper around the Confluent serializers.
 */
@Slf4j
public class ConfluentSerializerInternal<T> implements Serializer<T> {
    private Serializer<T> inner;
    private SerializationTypes type;
    private Map<String, ?> config;
    private boolean isKey;

    public ConfluentSerializerInternal(SerializationTypes type) {
        this.type = type;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.config = configs;
        this.isKey = isKey;
        if (type != null) {
            this.inner = getSerializerInstance(type);
            this.inner.configure(configs, isKey);
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (inner == null) {
            init(data);
        }

        return inner.serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, T data) {
        if (inner == null) {
            init(data);
        }

        if (headers != null) {
            type.toHeaders(headers, isKey);
        }

        return inner.serialize(topic, headers, data);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
            inner = null;
        }
    }

    @Synchronized
    private void init(T data) {
        if (inner != null) {
            return;
        }

        boolean srConfigured = config.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        type = TypeDetectionService.detectFromInstance(data, srConfigured);

        inner = getSerializerInstance(type);
        inner.configure(config, isKey);
    }

    protected Serializer<T> getSerializerInstance(SerializationTypes serializationTypes) {
        return SerdeFactory.createSerializer(serializationTypes);
    }
}
