package csid.client.connect.internal;

import csid.client.common.SerializationTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

/**
 * This class is a wrapper around the Confluent converters.
 */
@Slf4j
public class ConfluentValueConverterInternal implements Converter {

    private final Converter inner;
    private final SerializationTypes type;
    private final boolean isKey;

    public ConfluentValueConverterInternal(Converter inner, SerializationTypes type, boolean isKey) {
        this.inner = inner;
        this.type = type;
        this.isKey = isKey;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.inner.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return inner.fromConnectData(topic, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        if (headers != null) {
            type.toHeaders(headers, isKey);
        }
        
        return inner.fromConnectData(topic, headers, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return inner.toConnectData(topic, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return inner.toConnectData(topic, headers, value);
    }

    @Override
    public ConfigDef config() {
        return Converter.super.config();
    }

}
