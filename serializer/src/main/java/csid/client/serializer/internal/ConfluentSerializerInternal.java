/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package csid.client.serializer.internal;

import com.google.protobuf.Message;
import csid.client.common.SerializationTypes;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import java.util.UUID;

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

        if (data instanceof String) {
            type = SerializationTypes.String;
        } else if (data instanceof byte[]) {
            type = SerializationTypes.ByteArray;
        } else if (data instanceof Short) {
            type = SerializationTypes.Short;
        } else if (data instanceof Integer) {
            type = SerializationTypes.Integer;
        } else if (data instanceof Long) {
            type = SerializationTypes.Long;
        } else if (data instanceof Float) {
            type = SerializationTypes.Float;
        } else if (data instanceof Double) {
            type = SerializationTypes.Double;
        } else if (data instanceof Bytes) {
            type = SerializationTypes.Bytes;
        } else if (data instanceof UUID) {
            type = SerializationTypes.UUID;
        } else if (data instanceof ByteBuffer) {
            type = SerializationTypes.ByteBuffer;
        } else if (data instanceof Message) {
            type = SerializationTypes.Protobuf;
        } else if (data instanceof IndexedRecord) {
            type = SerializationTypes.Avro;
        } else {
            // Check if SR was configured
            if (config.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)) {
                type = SerializationTypes.JsonSchema;
            } else {
                type = SerializationTypes.Json;
            }
        }

        inner = getSerializerInstance(type);
        inner.configure(config, isKey);
    }

    @SuppressWarnings("unchecked")
    protected Serializer<T> getSerializerInstance(SerializationTypes serializationTypes) {
        switch (serializationTypes) {
            case String:
                log.info("String detected, using String serializer.");
                return (Serializer<T>) new StringSerializer();
            case Bytes:
                log.info("Bytes detected, using Bytes serializer.");
                return (Serializer<T>) new BytesSerializer();
            case ByteArray:
                log.info("Byte array detected, using ByteArray serializer.");
                return (Serializer<T>) new ByteArraySerializer();
            case ByteBuffer:
                log.info("ByteBuffer detected, using ByteBuffer serializer.");
                return (Serializer<T>) new ByteBufferSerializer();
            case Short:
                log.info("Short detected, using Short serializer.");
                return (Serializer<T>) new ShortSerializer();
            case Integer:
                log.info("Integer detected, using Integer serializer.");
                return (Serializer<T>) new IntegerSerializer();
            case Long:
                log.info("Long detected, using Long serializer.");
                return (Serializer<T>) new LongSerializer();
            case Float:
                log.info("Float detected, using Float serializer.");
                return (Serializer<T>) new FloatSerializer();
            case Double:
                log.info("Double detected, using Double serializer.");
                return (Serializer<T>) new DoubleSerializer();
            case UUID:
                log.info("UUID detected, using UUID serializer.");
                return (Serializer<T>) new UUIDSerializer();
            case Protobuf:
                log.info("Protobuf message detected, using Protobuf serializer.");
                return (Serializer<T>) new KafkaProtobufSerializer<>();
            case Avro:
                log.info("Avro record detected, using Avro serializer.");
                return (Serializer<T>) new KafkaAvroSerializer();
            case JsonSchema:
                log.info("Schema Registry configured, using JSON Schema serializer.");
                return new KafkaJsonSchemaSerializer<>();
            default:
            case Json:
                log.warn("Schema Registry not configured, falling back to JSON schemaless serializer. " +
                        "If you want to use JSON Schema, configure the serializer with the " +
                        "appropriate key/value serializers and the " + AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG + " property.");
                return new KafkaJsonSerializer<>();
        }
    }
}
