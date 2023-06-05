/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.deserializer;

import csid.client.deserializer.internal.ConfluentDeserializerCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * KafkaSmartDeserializer is a wrapper around the Confluent Kafka Deserializers.
 *
 * @param <T> The type of the deserialized object.
 */
@Slf4j
public class ConfluentDeserializer implements Deserializer<Object> {
    private final ConfluentDeserializerCache cache = new ConfluentDeserializerCache();
    private final Class<?> tClass;

    public ConfluentDeserializer() {
        tClass = null;
    }

    public ConfluentDeserializer(Class<?> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.cache.configure(configs, isKey, tClass);
    }

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        return cache.get(bytes, null).deserialize(topic, bytes);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] bytes) {
        return cache.get(bytes, headers).deserialize(topic, headers, bytes);
    }

    @Override
    public void close() {
        cache.close();
    }
}
