package csid.client;

import csid.client.schema.SchemaRegistryUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.IOException;
import java.util.function.Supplier;

@Slf4j
public enum SerializationTypes {
    String,
    Bytes,
    ByteArray,
    ByteBuffer,
    Short,
    Integer,
    Long,
    Float,
    Double,
    UUID,
    Protobuf,
    Avro,
    JsonSchema,
    Json;

    public static final String HEADER_KEY = "serialization.type";

    public void toHeaders(final Headers headers) {
        headers.add(HEADER_KEY, this.name().getBytes());
    }

    public static SerializationTypes fromHeaders(final Headers headers) {
        final Header header = headers.lastHeader(HEADER_KEY);
        return (header != null)
                ? SerializationTypes.valueOf(new String(header.value()))
                : null;
    }

    public static SerializationTypes fromString(String value) {
        for (SerializationTypes type : SerializationTypes.values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }

        return Avro;
    }

    public static SerializationTypes fromSchema(Supplier<SchemaRegistryClient> clientSupplier, final byte[] bytes) throws RestClientException, IOException {
        final String schemaType = SchemaRegistryUtils.getSchemaType(clientSupplier, bytes);
        if (StringUtils.isEmpty(schemaType)) {
            return null;
        }

        switch (schemaType) {
            case "AVRO":
                log.info("Schema type is AVRO");
                return Avro;
            case "JSON":
                log.info("Schema type is JSON");
                return JsonSchema;
            case "PROTOBUF":
                log.info("Schema type is PROTOBUF");
                return Protobuf;
            default:
                return null;
        }
    }
}
