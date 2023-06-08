/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package csid.client.deserializer.internal;

import csid.client.common.SerializationTypes;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;

import java.util.Map;
import java.util.function.Supplier;

/**
 * KafkaSmartDeserializer is a wrapper around the Confluent Kafka Deserializers.
 *
 * @param <T> The type of the deserialized object.
 */
@Slf4j
public class ConfluentDeserializerInternal<T> implements Deserializer<T> {
    private final Deserializer<?> inner;

    public ConfluentDeserializerInternal(final SerializationTypes serializationTypes, final Supplier<SchemaRegistryClient> srSupplier) {
        this.inner = getDeserializer(serializationTypes, srSupplier);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (inner != null) {
            inner.configure(configs, isKey);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String s, byte[] bytes) {
        return (T) inner.deserialize(s, bytes);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        return (T) inner.deserialize(topic, headers, bytes);
    }

    @Override
    public void close() {
        if (inner != null) {
            inner.close();
        }
    }

    /**
     * Get the deserializer for the given serialization type.
     *
     * @param serializationType The serialization type.
     * @return The deserializer.
     */
    private Deserializer<?> getDeserializer(SerializationTypes serializationType, Supplier<SchemaRegistryClient> supplier) {
        switch (serializationType) {
            case Avro:
                return new KafkaAvroDeserializer(supplier.get());
            case Json:
                return new KafkaJsonDeserializer<>();
            case JsonSchema:
                return new KafkaJsonSchemaDeserializer<>(supplier.get());
            case Protobuf:
                return new KafkaProtobufDeserializer<>(supplier.get());
            case Long:
                return new LongDeserializer();
            case Integer:
                return new IntegerDeserializer();
            case Float:
                return new FloatDeserializer();
            case Double:
                return new DoubleDeserializer();
            case Short:
                return new ShortDeserializer();
            case UUID:
                return new UUIDDeserializer();
            case String:
                return new StringDeserializer();
            case Bytes:
                return new BytesDeserializer();
            case ByteBuffer:
                return new ByteBufferDeserializer();
            case ByteArray:
            default:
                return new ByteArrayDeserializer();
        }
    }

}
