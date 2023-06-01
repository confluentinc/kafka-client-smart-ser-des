package csid.client.serializer.internal;

import csid.client.common.SerializationTypes;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to cache the serializers.
 */
@Slf4j
public class ConfluentSerializerCache implements Closeable {

    private final Map<SerializationTypes, ConfluentSerializerInternal<Object>> cache = new ConcurrentHashMap<>();
    private Map<String, Object> configs;
    private boolean isKey;
    private boolean srConfigured;

    /**
     * Get the serializer for the given data.
     *
     * @param data The data to serialize.
     * @return The serializer.
     */
    public Serializer<Object> get(Object data) {
        final SerializationTypes serializationType = SerializationTypes.fromClass(data, srConfigured);
        return (serializationType == null)
                ? get(SerializationTypes.Json)
                : get(serializationType);
    }

    public void configure(Map<String, ?> configs, final boolean isKey) {
        this.configs = new HashMap<>(configs);
        // If the user has not configured a subject name strategy, we will use the topic name strategy.
        if (isKey) {
            if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY)) {
                this.configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
            }
        } else if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY)) {
            this.configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        }

        this.isKey = isKey;
        this.srConfigured = configs.containsKey("schema.registry.url");
    }

    /**
     * Get the serializer for the given serialization type.
     *
     * @param serializationType The serialization type.
     * @return The serializer.
     */
    private Serializer<Object> get(SerializationTypes serializationType) {
        if (!cache.containsKey(serializationType)) {
            init(serializationType);
        }

        return cache.get(serializationType);
    }

    /**
     * This method is synchronized to avoid multiple threads to initialize the same serializer.
     *
     * @param serializationType The serialization type.
     */
    @Synchronized
    private void init(SerializationTypes serializationType) {
        if (cache.containsKey(serializationType)) {
            return;
        }

        ConfluentSerializerInternal<Object> serializer = new ConfluentSerializerInternal<>(serializationType);
        serializer.configure(configs, isKey);

        cache.put(serializationType, serializer);
    }

    @Override
    @Synchronized
    public void close() {
        cache.forEach((k, v) -> v.close());
        cache.clear();
    }

}
