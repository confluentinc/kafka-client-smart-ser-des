package csid.client.common.caching;

import csid.client.common.SerializationTypes;
import csid.client.common.exception.ConfluentSchemaException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class ConfluentSerdeCache<T> {

    public interface SerdeFactory<T> {
        T create(SerializationTypes serializationTypes, Map<String, ?> configs, boolean isKey);
    }

    private final Map<SerializationTypes, T> cache = new ConcurrentHashMap<>();
    private final Map<String, Object> configs;
    private final boolean isKey;
    private final boolean srConfigured;
    private final SerdeFactory<T> factory;
    private final Supplier<SchemaRegistryClient> srSupplier;
    private final SerializationTypes defaultType;

    public ConfluentSerdeCache(SerdeFactory<T> factory, Supplier<SchemaRegistryClient> srSupplier, Map<String, ?> configs, boolean isKey, SerializationTypes defaultType) {
        this.factory = factory;
        this.srSupplier = srSupplier;
        this.configs = new HashMap<>(configs);
        this.isKey = isKey;
        this.srConfigured = configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        this.defaultType = defaultType;


        // If the user has not configured a subject name strategy, we will use the topic name strategy.
        if (isKey) {
            if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY)) {
                this.configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
            }
        } else if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY)) {
            this.configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        }
    }


    public ConfluentSerdeCache(SerdeFactory<T> factory, Supplier<SchemaRegistryClient> srSupplier, Map<String, ?> configs, boolean isKey) {
        this(factory, srSupplier, configs, isKey, null);
    }

    public T get(final byte[] data, final Headers headers) {
        SerializationTypes serializationType = SerializationTypes.fromHeaders(headers, isKey);
        if (serializationType == null) {
            try {
                if (defaultType != null) {
                    serializationType = defaultType;
                } else {
                    serializationType = SerializationTypes.fromBytes(srSupplier, data);
                }
            } catch (RestClientException | IOException e) {
                throw new ConfluentSchemaException("Could not determine the serialization type.", e);
            }
        }

        if (serializationType == null) {
            return null;
        }

        return get(serializationType);
    }

    public T get(Object data) {
        final SerializationTypes serializationType = SerializationTypes.fromClass(data, srConfigured);
        return (serializationType == null)
                ? get(defaultType)
                : get(serializationType);
    }

    public T get(byte[] bytes) throws RestClientException, IOException {
        final SerializationTypes serializationType = SerializationTypes.fromBytes(srSupplier, bytes);
        return (serializationType == null)
                ? get(defaultType)
                : get(serializationType);
    }

    public T get(Class<?> tClass) {
        final SerializationTypes serializationType = SerializationTypes.fromClass(tClass, srConfigured);
        return (serializationType == null)
                ? get(defaultType)
                : get(serializationType);
    }

    public T get(Object data, boolean isPrimitive) {
        final SerializationTypes serializationTypes = isPrimitive
                ? SerializationTypes.fromClass(data.getClass())
                : defaultType;
        return get(serializationTypes);
    }


    public void close() {
        cache.values().forEach(serde -> {
            if (serde instanceof Closeable) {
                try {
                    ((Closeable) serde).close();
                } catch (IOException e) {
                    // Ignore
                }
            }
        });
    }

    private T get(SerializationTypes serializationType) {
        return cache.computeIfAbsent(serializationType, type -> factory.create(type, configs, isKey));
    }
}
