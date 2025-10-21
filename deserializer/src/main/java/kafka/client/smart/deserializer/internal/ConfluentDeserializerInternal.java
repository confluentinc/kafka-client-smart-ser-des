/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.deserializer.internal;

import kafka.client.smart.common.SerializationTypes;
import kafka.client.smart.common.SerdeFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
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
        return SerdeFactory.createDeserializer(serializationType, supplier);
    }

}
