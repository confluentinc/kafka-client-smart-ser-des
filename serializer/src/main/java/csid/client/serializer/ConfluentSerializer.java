/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.serializer;

import csid.client.serializer.internal.ConfluentSerializerCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

import java.util.Map;


/**
 * A serializer that can serialize objects of any type using Confluent's serializers.
 */
@Slf4j
public class ConfluentSerializer implements Serializer<Object> {
    private final ConfluentSerializerCache cache = new ConfluentSerializerCache();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        cache.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        return cache.get(data).serialize(topic, data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return cache.get(data).serialize(topic, headers, data);
    }

    @Override
    public void close() {
        cache.close();
    }
}