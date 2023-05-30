package csid.client;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

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
}
