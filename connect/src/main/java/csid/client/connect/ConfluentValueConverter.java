package csid.client.connect;

import csid.client.connect.exceptions.ConfluentValueConverterException;
import csid.client.connect.internal.ConfluentValueConverterCache;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.io.IOException;
import java.util.Map;

/**
 * This class is a wrapper around the Confluent converters.
 */
@Slf4j
public class ConfluentValueConverter implements Converter {

    private final ConfluentValueConverterCache cache = new ConfluentValueConverterCache();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        cache.configure(configs, isKey);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return cache.getFromObject(value, schema).fromConnectData(topic, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
        return cache.getFromObject(value, schema).fromConnectData(topic, headers, schema, value);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return cache.getFromBytes(value, null).toConnectData(topic, value);
        } catch (RestClientException | IOException e) {
            throw new ConfluentValueConverterException("Error during inner converter initialization.", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        try {
            return cache.getFromBytes(value, headers).toConnectData(topic, value);
        } catch (RestClientException | IOException e) {
            throw new ConfluentValueConverterException("Error during inner converter initialization.", e);
        }
    }
}
