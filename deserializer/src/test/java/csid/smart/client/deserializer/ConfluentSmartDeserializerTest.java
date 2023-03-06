package csid.smart.client.deserializer;

import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;


import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;

public class ConfluentSmartDeserializerTest {

    public static final String TEST_TOPIC_NAME = "test-deserializer-topic";

    public static final int SHORT_TYPE_LENGTH = 2;
    public static final int FLOAT_TYPE_LENGTH = 4;
    public static final int INT_TYPE_LENGTH = 4;
    public static final int DOUBLE_TYPE_LENGTH = 8;
    public static final int LONG_TYPE_LENGTH = 8;
    public static final int BYTEBUFFER_CAPACITY = 10;

    @Test
    public void testDeserializeString() {
        // Given
        Properties props = new Properties();
        String expected = anyString();
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, String.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeByteArray() {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};

        // When
        Object actual = confluentDeserializer(props,false,  expected, byte[].class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeByteBuffer() {
        // Given
        Properties props = new Properties();
        ByteBuffer expected = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        byte[] expectedBytes = expected.array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, ByteBuffer.class);

        // Then
        assertEquals(expected, actual);

    }

    @Test
    public void testDeserializeFloat() {
        // Given
        Properties props = new Properties();
        float expected = anyFloat();
        byte[] expectedBytes = ByteBuffer.allocate(FLOAT_TYPE_LENGTH).putFloat(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Float.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeDouble() {
        // Given
        Properties props = new Properties();
        double expected = anyDouble();
        byte[] expectedBytes = ByteBuffer.allocate(DOUBLE_TYPE_LENGTH).putDouble(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Double.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeInt() {
        // Given
        Properties props = new Properties();
        int expected = anyInt();
        byte[] expectedBytes = ByteBuffer.allocate(INT_TYPE_LENGTH).putInt(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Integer.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeLong() {
        // Given
        Properties props = new Properties();
        long expected = anyLong();
        byte[] expectedBytes = ByteBuffer.allocate(LONG_TYPE_LENGTH).putLong(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Long.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeShort() {
        // Given
        Properties props = new Properties();
        short expected = anyShort();
        byte[] expectedBytes = ByteBuffer.allocate(SHORT_TYPE_LENGTH).putShort(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Short.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeBytes() {
        // Given
        Properties props = new Properties();
        Bytes expected = Bytes.wrap(new byte[]{anyByte()});
        byte[] expectedBytes = expected.get();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Bytes.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeUuid() {
        // Given
        Properties props = new Properties();
        UUID expected = UUID.fromString("f90ae889-2866-474c-b21b-88c98ea99515");
        byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, UUID.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeKafkaAvro() {
        // TODO: https://confluentinc.atlassian.net/browse/CCET-251
    }

    @Test
    public void testDeserializeKafkaJsonSchema() {
        // TODO: https://confluentinc.atlassian.net/browse/CCET-251
    }

    @Test
    public void testDeserializeKafkaJson() {
        // TODO: https://confluentinc.atlassian.net/browse/CCET-251
    }

    @Test
    public void testDeserializeKafkaProtobuf() {
        // TODO: https://confluentinc.atlassian.net/browse/CCET-251
    }

    private static Object confluentDeserializer(Properties props, boolean isKey , byte[] expected, Class<?> stringClass) {
        try (ConfluentSmartDeserializer<?> confluentSmartDeserializer = new ConfluentSmartDeserializer<>(props, isKey, stringClass)) {
            return confluentSmartDeserializer.deserialize(TEST_TOPIC_NAME, expected);
        }
    }
}