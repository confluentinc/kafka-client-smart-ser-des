package csid.client.connect.internal;

import csid.client.common.SerializationTypes;
import csid.client.common.schema.SchemaRegistryUtils;
import csid.client.connect.ConfluentValueConverterConfig;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.converters.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConfluentValueConverterCache is a cache for converters.
 */
@Slf4j
public class ConfluentValueConverterCache {

    private final Map<SerializationTypes, Converter> cache = new ConcurrentHashMap<>();
    private SerializationTypes defaultType;
    private boolean isKey;
    private SchemaRegistryClient schemaRegistryClient;
    private Map<String, Object> configs;

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configs = new HashMap<>(configs);

        // If the user has not configured a subject name strategy, we will use the topic name strategy.
        if (isKey) {
            if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY)) {
                this.configs.put(AbstractKafkaSchemaSerDeConfig.KEY_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
            }
        } else if (!this.configs.containsKey(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY)) {
            this.configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        }

        final ConfluentValueConverterConfig config = new ConfluentValueConverterConfig(configs);

        this.defaultType = config.getType();
        this.isKey = isKey;
    }

    /**
     * Get the inner converter.
     *
     * @param bytes   The bytes to be converted.
     * @param headers The headers of the message.
     * @return The inner converter.
     * @throws RestClientException If the schema registry client fails.
     * @throws IOException         If the schema registry client fails.
     */
    public Converter getFromBytes(byte[] bytes, final Headers headers) throws RestClientException, IOException {
        SerializationTypes serializationType = null;
        if (headers != null) {
            serializationType = SerializationTypes.fromHeaders(headers, isKey);
        }

        if (serializationType == null) {
            log.info("No serialization type found in headers. Trying to get it from the schema.");

            serializationType = SerializationTypes.fromBytes(this::getSchemaRegistryClient, bytes);

            log.info("Serialization type found in schema: {}", serializationType);
        }

        if (!cache.containsKey(serializationType)) {
            initInnerConverter(serializationType);
        }

        return cache.get(serializationType);
    }

    /**
     * Get the inner converter.
     *
     * @param data   The data to be converted.
     * @param schema The schema of the data.
     * @return The inner converter.
     */
    public Converter getFromObject(Object data, Schema schema) {
        final SerializationTypes serializationTypes = (schema.type().isPrimitive())
                ? SerializationTypes.fromClass(data.getClass())
                : defaultType;

        if (!cache.containsKey(serializationTypes)) {
            initInnerConverter(serializationTypes);
        }

        return cache.get(serializationTypes);
    }

    /**
     * Initialize inner converter.
     *
     * @param serializationType serialization type
     */
    private void initInnerConverter(final SerializationTypes serializationType) {
        final Converter converter;

        switch (serializationType) {
            case Avro:
                converter = new AvroConverter();
                break;
            case JsonSchema:
                converter = new JsonSchemaConverter();
                break;
            case Protobuf:
                converter = new ProtobufConverter();
                break;
            case String:
                converter = new StringConverter();
                break;
            case Json:
                converter = new JsonConverter();
                break;
            case Short:
                converter = new ShortConverter();
                break;
            case Integer:
                converter = new IntegerConverter();
                break;
            case Long:
                converter = new LongConverter();
                break;
            case Double:
                converter = new DoubleConverter();
                break;
            case Float:
                converter = new FloatConverter();
                break;
            default:
                converter = new ByteArrayConverter();
                break;
        }

        converter.configure(configs, isKey);
        cache.put(serializationType, new ConfluentValueConverterInternal(converter, serializationType, isKey));
    }


    /**
     * Get the schema registry client.
     *
     * @return The schema registry client.
     */
    @Synchronized
    private SchemaRegistryClient getSchemaRegistryClient() {
        if (schemaRegistryClient == null) {
            schemaRegistryClient = SchemaRegistryUtils.getSchemaRegistryClient(configs);
        }

        return schemaRegistryClient;
    }

}
