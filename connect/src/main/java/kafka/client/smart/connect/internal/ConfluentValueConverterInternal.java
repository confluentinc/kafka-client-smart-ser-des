/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.connect.internal;

import kafka.client.smart.common.SerializationTypes;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.json.JsonSchemaConverter;
import io.confluent.connect.protobuf.ProtobufConverter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.converters.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.StringConverter;

import java.util.Map;

/**
 * This class is a wrapper around the Confluent converters.
 */
@Slf4j
public class ConfluentValueConverterInternal implements Converter {

    private Converter inner;
    private final SerializationTypes type;
    private final boolean isKey;

    public ConfluentValueConverterInternal(SerializationTypes type, boolean isKey) {
        this.type = type;
        this.isKey = isKey;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        switch (type) {
            case Avro:
                this.inner = new AvroConverter();
                break;
            case JsonSchema:
                this.inner = new JsonSchemaConverter();
                break;
            case Protobuf:
                this.inner = new ProtobufConverter();
                break;
            case String:
                this.inner = new StringConverter();
                break;
            case Json:
                this.inner = new JsonConverter();
                break;
            case Short:
                this.inner = new ShortConverter();
                break;
            case Integer:
                this.inner = new IntegerConverter();
                break;
            case Long:
                this.inner = new LongConverter();
                break;
            case Double:
                this.inner = new DoubleConverter();
                break;
            case Float:
                this.inner = new FloatConverter();
                break;
            default:
                this.inner = new ByteArrayConverter();
                break;
        }

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
