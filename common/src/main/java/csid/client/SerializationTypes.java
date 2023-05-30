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

/**
 * This enum is used to determine the serialization type of the message.
 */
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

    /**
     * Set the serialization type in the headers.
     *
     * @param headers The headers
     */
    public void toHeaders(final Headers headers) {
        headers.add(HEADER_KEY, this.name().getBytes());
    }

    /**
     * This method is used to determine the serialization type from the headers.
     *
     * @param headers The headers
     * @return The serialization type
     */
    public static SerializationTypes fromHeaders(final Headers headers) {
        final Header header = headers.lastHeader(HEADER_KEY);
        return (header != null)
                ? SerializationTypes.valueOf(new String(header.value()))
                : null;
    }

    /**
     * This method is used to determine the serialization type from a string.
     *
     * @param value The string value
     * @return The serialization type
     */
    public static SerializationTypes fromString(String value) {
        for (SerializationTypes type : SerializationTypes.values()) {
            if (type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }

        return Avro;
    }

    /**
     * This method is used to determine the serialization type of the message.
     *
     * @param clientSupplier Supplier of the Schema Registry Client
     * @param bytes          The message bytes
     * @return The schema type
     * @throws RestClientException If the Schema Registry Client fails
     * @throws IOException         If the Schema Registry Client fails
     */
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
