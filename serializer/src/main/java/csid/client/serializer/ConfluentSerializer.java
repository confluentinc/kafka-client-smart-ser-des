/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.serializer;

import com.google.protobuf.Message;
import csid.client.common.SerializationTypes;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
public class ConfluentSerializer<T> implements Serializer<T> {
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private Serializer<T> inner;
    private SerializationTypes type;
    private Map<String, ?> config;
    private boolean isKey;

    public ConfluentSerializer() {
    }

    public ConfluentSerializer(Properties properties, boolean isKey) {
        this.config = properties.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> e.getKey().toString(), Map.Entry::getValue));
        this.isKey = isKey;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.config = configs;
        this.isKey = isKey;
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
            type.toHeaders(headers);
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
    @SuppressWarnings("unchecked")
    private void init(T data) {
        if (inner != null) {
            return;
        }

        if (data instanceof String) {
            log.info("String detected, using String serializer.");
            inner = (Serializer<T>) new StringSerializer();
            type = SerializationTypes.String;
        } else if (data instanceof byte[]) {
            log.info("Byte array detected, using ByteArray serializer.");
            inner = (Serializer<T>) new ByteArraySerializer();
            type = SerializationTypes.ByteArray;
        } else if (data instanceof Short) {
            log.info("Short detected, using Short serializer.");
            inner = (Serializer<T>) new ShortSerializer();
            type = SerializationTypes.Short;
        } else if (data instanceof Integer) {
            log.info("Integer detected, using Integer serializer.");
            inner = (Serializer<T>) new IntegerSerializer();
            type = SerializationTypes.Integer;
        } else if (data instanceof Long) {
            log.info("Long detected, using Long serializer.");
            inner = (Serializer<T>) new LongSerializer();
            type = SerializationTypes.Long;
        } else if (data instanceof Float) {
            log.info("Float detected, using Float serializer.");
            inner = (Serializer<T>) new FloatSerializer();
            type = SerializationTypes.Float;
        } else if (data instanceof Double) {
            log.info("Double detected, using Double serializer.");
            inner = (Serializer<T>) new DoubleSerializer();
            type = SerializationTypes.Double;
        } else if (data instanceof Bytes) {
            log.info("Bytes detected, using Bytes serializer.");
            inner = (Serializer<T>) new BytesSerializer();
            type = SerializationTypes.Bytes;
        } else if (data instanceof UUID) {
            log.info("UUID detected, using UUID serializer.");
            inner = (Serializer<T>) new UUIDSerializer();
            type = SerializationTypes.UUID;
        } else if (data instanceof ByteBuffer) {
            log.info("ByteBuffer detected, using ByteBuffer serializer.");
            inner = (Serializer<T>) new ByteBufferSerializer();
            type = SerializationTypes.ByteBuffer;
        } else if (data instanceof Message) {
            log.info("Protobuf message detected, using Protobuf serializer.");
            inner = (Serializer<T>) new KafkaProtobufSerializer<>();
            type = SerializationTypes.Protobuf;
        } else if (data instanceof IndexedRecord) {
            log.info("Avro record detected, using Avro serializer.");
            inner = (Serializer<T>) new KafkaAvroSerializer();
            type = SerializationTypes.Avro;
        } else {
            // Check if SR was configured
            if (config.containsKey(SCHEMA_REGISTRY_URL)) {
                log.info("Schema Registry configured, using JSON Schema serializer.");
                inner = new KafkaJsonSchemaSerializer<>();
                type = SerializationTypes.JsonSchema;
            } else {
                log.warn("Schema Registry not configured, falling back to JSON schemaless serializer. " +
                        "If you want to use JSON Schema, configure the serializer with the " +
                        "appropriate key/value serializers and the " + SCHEMA_REGISTRY_URL + " property.");
                inner = new KafkaJsonSerializer<>();
                type = SerializationTypes.Json;
            }
        }

        inner.configure(config, isKey);
    }
}
