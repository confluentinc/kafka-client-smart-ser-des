package csid.client.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import csid.client.serializer.record.OrderRecord;
import csid.client.serializer.record.OrderSchemaRecord;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;

class ConfluentSerializerTest {

    public static final String TEST_TOPIC_NAME = "test-serializer-topic";

    private ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testSerializeString() {
        // Given
        Properties props = new Properties();
        String expected = anyString();

        // When
        byte[] actual = confluentSerializer(props, false, expected);

        // Then
        assertEquals(expected, new String(actual));
    }

    @Test
    public void testSerializeByteArray() {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};

        // When
        byte[] actual = confluentSerializer(props, false, expected);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeShort() {
        // Given
        Properties props = new Properties();
        short expected = anyShort();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        // Then
        assertEquals(expected, actual.getShort());
    }

    @Test
    public void testSerializeInt() {
        // Given
        Properties props = new Properties();
        int expected = anyInt();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        // Then
        assertEquals(expected, actual.getInt());
    }

    @Test
    public void testSerializeLong() {
        // Given
        Properties props = new Properties();
        long expected = anyLong();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        // Then
        assertEquals(expected, actual.getLong());
    }

    @Test
    public void testSerializeFloat() {
        // Given
        Properties props = new Properties();
        float expected = anyLong();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        // Then
        assertEquals(expected, actual.getFloat());
    }

    @Test
    public void testSerializeDouble() {
        // Given
        Properties props = new Properties();
        double expected = anyLong();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);

        // Then
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        assertEquals(expected, actual.getDouble());
    }

    @Test
    public void testSerializeBytes() {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};

        // When
        byte[] actual = confluentSerializer(props, false, expected);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeUuid() {
        // Given
        Properties props = new Properties();
        UUID expected = UUID.fromString("f90ae889-2866-474c-b21b-88c98ea99515");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        UUID actual = UUID.fromString(new String(actualBytes));

        // Then
        assertEquals(expected, actual);
    }


    @Test
    public void testSerializeKafkaJsonSchema() throws RestClientException, IOException {
        // Given
        Properties props = new Properties();
        OrderSchemaRecord expected = new OrderSchemaRecord("test", "test", "test", "test");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        OrderSchemaRecord actual = MAPPER.readValue(actualBytes, OrderSchemaRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeKafkaJson() throws IOException {
        // Given
        Properties props = new Properties();
        OrderRecord expected = new OrderRecord("test", "test", "test", "test");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected);
        OrderRecord actual = MAPPER.readValue(actualBytes, OrderRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    private static <T> byte[] confluentSerializer(Properties props, boolean isKey, T expected) {
        try (ConfluentSerializer<T> confluentSerializer = new ConfluentSerializer<>(props, isKey)) {
            return confluentSerializer.serialize(TEST_TOPIC_NAME, expected);
        }
    }
}