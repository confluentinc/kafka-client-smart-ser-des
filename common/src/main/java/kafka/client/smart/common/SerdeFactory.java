/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.*;

import java.util.function.Supplier;

/**
 * Centralized factory for creating serializers and deserializers.
 * This eliminates code duplication between ConfluentSerializerInternal and ConfluentDeserializerInternal.
 */
@Slf4j
public final class SerdeFactory {

    private SerdeFactory() {
        // Utility class
    }

    /**
     * Create a serializer for the given serialization type.
     *
     * @param serializationType The serialization type
     * @param <T> The type of object to serialize
     * @return The appropriate serializer
     */
    @SuppressWarnings("unchecked")
    public static <T> Serializer<T> createSerializer(SerializationTypes serializationType) {
        switch (serializationType) {
            case String:
                log.info("String detected, using String serializer.");
                return (Serializer<T>) new StringSerializer();
            case Boolean:
                log.info("Boolean detected, using Boolean serializer");
                return (Serializer<T>) new BooleanSerializer();
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

    /**
     * Create a deserializer for the given serialization type.
     *
     * @param serializationType The serialization type
     * @param srSupplier Supplier for Schema Registry client
     * @param <T> The type of object to deserialize
     * @return The appropriate deserializer
     */
    @SuppressWarnings("unchecked")
    public static <T> Deserializer<T> createDeserializer(SerializationTypes serializationType, Supplier<SchemaRegistryClient> srSupplier) {
        switch (serializationType) {
            case Avro:
                return (Deserializer<T>) new KafkaAvroDeserializer(srSupplier.get());
            case Json:
                return (Deserializer<T>) new KafkaJsonDeserializer<>();
            case JsonSchema:
                return (Deserializer<T>) new KafkaJsonSchemaDeserializer<>(srSupplier.get());
            case Protobuf:
                return (Deserializer<T>) new KafkaProtobufDeserializer<>(srSupplier.get());
            case Long:
                return (Deserializer<T>) new LongDeserializer();
            case Integer:
                return (Deserializer<T>) new IntegerDeserializer();
            case Float:
                return (Deserializer<T>) new FloatDeserializer();
            case Double:
                return (Deserializer<T>) new DoubleDeserializer();
            case Short:
                return (Deserializer<T>) new ShortDeserializer();
            case UUID:
                return (Deserializer<T>) new UUIDDeserializer();
            case String:
                return (Deserializer<T>) new StringDeserializer();
            case Bytes:
                return (Deserializer<T>) new BytesDeserializer();
            case ByteBuffer:
                return (Deserializer<T>) new ByteBufferDeserializer();
            case ByteArray:
            default:
                return (Deserializer<T>) new ByteArrayDeserializer();
        }
    }
}
