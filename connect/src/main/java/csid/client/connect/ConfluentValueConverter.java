package csid.client.connect;

import csid.client.common.SerializationTypes;
import csid.client.connect.exceptions.ConfluentValueConverterException;
import csid.client.common.schema.SchemaRegistryUtils;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.converters.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.io.IOException;
import java.util.Map;

/**
 * This class is a wrapper around the Confluent converters.
 */
@Slf4j
public class ConfluentValueConverter implements Converter {

    private Converter inner;

    private Map<String, ?> configs;
    private boolean isKey;
    private SchemaRegistryClient schemaRegistryClient;
    private SerializationTypes type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        final ConfluentValueConverterConfig config = new ConfluentValueConverterConfig(configs);

        this.type = config.getType();
        this.configs = configs;
        this.isKey = isKey;
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (inner == null) {
            initFromConnectData();
        }

        return inner.fromConnectData(topic, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        if (inner == null) {
            initFromConnectData();
        }

        if (headers != null) {
            type.toHeaders(headers);
        }

        return inner.fromConnectData(topic, headers, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (inner == null) {
            try {
                initToConnectData(value, null);
            } catch (RestClientException | IOException e) {
                throw new ConfluentValueConverterException("Error during inner converter initialization.", e);
            }
        }

        return inner.toConnectData(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        if (inner == null) {
            try {
                initToConnectData(value, headers);
            } catch (RestClientException | IOException e) {
                throw new ConfluentValueConverterException("Error during inner converter initialization.", e);
            }
        }

        return inner.toConnectData(topic, headers, value);
    }

    @Override
    public ConfigDef config() {
        return Converter.super.config();
    }

    /**
     * Initialize converter when converting from kafka to connect.
     *
     * @param bytes   bytes to convert
     * @param headers headers to convert
     * @throws RestClientException if an error occurs while getting the schema
     * @throws IOException         if an error occurs while getting the schema
     */
    @Synchronized
    private void initToConnectData(byte[] bytes, final Headers headers) throws RestClientException, IOException {
        if (inner != null) {
            return;
        }

        SerializationTypes serializationType = null;
        if (headers != null) {
            serializationType = SerializationTypes.fromHeaders(headers);
        }

        if (serializationType == null) {
            log.info("No serialization type found in headers. Trying to get it from the schema.");

            serializationType = SerializationTypes.fromBytes(this::getSchemaRegistryClient, bytes);

            log.info("Serialization type found in schema: {}", serializationType);
        }
        
        initInnerConverter(serializationType);
    }

    /**
     * Initialize converter when converting from connect to kafka.
     */
    @Synchronized
    private void initFromConnectData() {
        if (inner != null) {
            return;
        }

        initInnerConverter(type);
    }

    /**
     * Initialize inner converter.
     *
     * @param serializationType serialization type
     */
    private void initInnerConverter(final SerializationTypes serializationType) {
        switch (serializationType) {
            case Avro:
                inner = new AvroConverter();
                break;
            case JsonSchema:
                inner = new JsonSchemaConverter();
                break;
            case Protobuf:
                inner = new ProtobufConverter();
                break;
            case String:
                inner = new StringConverter();
                break;
            case Json:
                inner = new JsonConverter();
                break;
            case Short:
                inner = new ShortConverter();
                break;
            case Integer:
                inner = new IntegerConverter();
                break;
            case Long:
                inner = new LongConverter();
                break;
            case Double:
                inner = new DoubleConverter();
                break;
            case Float:
                inner = new FloatConverter();
                break;
            default:
                inner = new ByteArrayConverter();
                break;
        }

        inner.configure(configs, isKey);
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
