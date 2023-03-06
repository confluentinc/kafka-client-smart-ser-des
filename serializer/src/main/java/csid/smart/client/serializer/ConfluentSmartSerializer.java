/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.smart.client.serializer;

import com.google.protobuf.Message;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import lombok.Synchronized;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConfluentSmartSerializer<T> implements Serializer<T> {

    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    @SuppressWarnings("rawtypes")
    private Serializer inner;
    private Map<String, ?> config;
    private boolean isKey;

    public ConfluentSmartSerializer() {
    }

    public ConfluentSmartSerializer(Properties properties, boolean isKey) {
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
    @SuppressWarnings("unchecked")
    public byte[] serialize(String topic, T data) {
        if (inner == null) {
            init(data);
        }

        return inner.serialize(topic, data);
    }

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serialize(String topic, Headers headers, T data) {
        if (inner == null) {
            init(data);
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
            inner = new StringSerializer();
        } else if (data instanceof byte[]) {
            inner = new ByteArraySerializer();
        } else if (data instanceof Short) {
            inner = new ShortSerializer();
        } else if (data instanceof Integer) {
            inner = new IntegerSerializer();
        } else if (data instanceof Long) {
            inner = new LongSerializer();
        } else if (data instanceof Float) {
            inner = new FloatSerializer();
        } else if (data instanceof Double) {
            inner = new DoubleSerializer();
        } else if (data instanceof Bytes) {
            inner = new BytesSerializer();
        } else if (data instanceof UUID) {
            inner = new UUIDSerializer();
        } else if (data instanceof Message) {
            inner = new KafkaProtobufSerializer<>();
        } else if (data instanceof IndexedRecord) {
            inner = new KafkaAvroSerializer();
        } else {
            // Check if SR was configured
            if (config.containsKey(SCHEMA_REGISTRY_URL)) {
                inner = new KafkaJsonSchemaSerializer<T>();
            } else {
                inner = new KafkaJsonSerializer<T>();
            }
        }

        inner.configure(config, isKey);
    }
}
