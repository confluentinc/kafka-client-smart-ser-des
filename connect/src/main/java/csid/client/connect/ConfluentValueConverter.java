package csid.client.connect;

import csid.client.connect.exceptions.ConfluentValueConverterException;
import csid.client.schema.SchemaRegistryUtils;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.converters.IntegerConverter;
import org.apache.kafka.connect.converters.LongConverter;
import org.apache.kafka.connect.converters.ShortConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.io.IOException;
import java.util.Map;

/**
 * SmartValueConverter
 */
@Slf4j
public class ConfluentValueConverter implements Converter {

    private Converter inner;

    private Map<String, ?> configs;
    private boolean isKey;
    private SchemaRegistryClient schemaRegistryClient;
    private ConfluentValueConverterTypes type;

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

        return inner.fromConnectData(topic, headers, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (inner == null) {
            initToConnectData(value);
        }

        return inner.toConnectData(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        if (inner == null) {
            initToConnectData(value);
        }

        return inner.toConnectData(topic, headers, value);
    }

    @Override
    public ConfigDef config() {
        return Converter.super.config();
    }

    /**
     * Initialize converter when converting from kafka to connect data.
     *
     * @param bytes payload
     */
    @Synchronized
    private void initToConnectData(byte[] bytes) {
        if (inner != null) {
            return;
        }

        // Get schema id from payload.
        final int schemaId = SchemaRegistryUtils.getSchemaId(bytes);
        if (schemaId != SchemaRegistryUtils.NO_SCHEMA_ID) {
            if (schemaRegistryClient == null) {
                schemaRegistryClient = SchemaRegistryUtils.getSchemaRegistryClient(configs);
            }

            // Schema base payload.
            ParsedSchema parsedSchema;
            try {
                parsedSchema = this.schemaRegistryClient.getSchemaById(schemaId);
            } catch (IOException | RestClientException e) {
                log.error("Failed to get schema by id: {}", schemaId, e);
                throw new ConfluentValueConverterException(String.format("Failed to get schema by id: %s", schemaId), e);
            }
            switch (parsedSchema.schemaType()) {
                case "AVRO":
                    log.info("AVRO schema detected");
                    inner = new AvroConverter(schemaRegistryClient);
                    break;
                case "JSON":
                    log.info("JSON schema detected");
                    inner = new JsonSchemaConverter(schemaRegistryClient);
                    break;
                case "PROTOBUF":
                    log.info("PROTOBUF schema detected");
                    inner = new ProtobufConverter(schemaRegistryClient);
                    break;
                default:
                    log.info("Unknown schema type detected");
                    inner = new ByteArrayConverter();
                    break;
            }
        } else if ((bytes[0] == '{' && bytes[bytes.length - 1] == '}') ||
                (bytes[0] == '[' && bytes[bytes.length - 1] == ']')) {
            // Maybe a JSON ?
            log.info("JSON detected");
            inner = new JsonConverter();
        } else if (bytes.length == 1) {
            log.info("Byte detected");
            inner = new ByteArrayConverter();
        } else if (bytes.length == 2) {
            log.info("Short detected");
            inner = new ShortConverter();
        } else if (bytes.length == 4) {
            log.info("Integer detected");
            inner = new IntegerConverter();
        } else if (bytes.length == 8) {
            log.info("Long detected");
            inner = new LongConverter();
        } else {
            log.info("String detected");
            inner = new StringConverter();
        }

        inner.configure(configs, isKey);
    }

    /**
     * Initialize converter when converting from connect to kafka.
     */
    @Synchronized
    private void initFromConnectData() {
        if (inner != null) {
            return;
        }

        switch (type) {
            case AVRO:
                inner = new AvroConverter();
                break;
            case JSON:
                inner = new JsonSchemaConverter();
                break;
            case PROTOBUF:
                inner = new ProtobufConverter();
                break;
            default:
                inner = new ByteArrayConverter();
                break;
        }

        inner.configure(configs, isKey);
    }
}
