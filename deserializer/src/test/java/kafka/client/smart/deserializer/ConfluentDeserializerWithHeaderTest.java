/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.deserializer;

import com.google.common.collect.Maps;
import kafka.client.smart.common.SerializationTypes;
import kafka.client.smart.deserializer.record.OrderRecord;
import kafka.client.smart.serializer.ConfluentSerializer;
import io.confluent.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static kafka.client.smart.deserializer.ConfluentDeserializerTest.generateOrderGenericRecord;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;

public class ConfluentDeserializerWithHeaderTest {

    public static final String TEST_TOPIC_NAME = "test-deserializer-topic";

    public static final String ORDERS_AVRO_SCHEMA = "schema/orders-avro.avsc";

    public static final int BYTEBUFFER_CAPACITY = 10;

    @Test
    public void testDeserializeWithHeader() {
        // Given
        Properties props = new Properties();
        String expected = "Sample String";

        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        final Object actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.String);

        // Then
        assertEquals(String.class, actual.getClass());
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeString() {
        // Given
        Properties props = new Properties();
        String expected = "Sample String";

        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        final String actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.String);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeByteArray() {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        byte[] actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.ByteArray);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeByteBuffer() {
        // Given
        Properties props = new Properties();
        ByteBuffer expected = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        ByteBuffer actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.ByteBuffer);

        // Then
        assertEquals(expected, actual);

    }

    @Test
    public void testDeserializeFloat() {
        // Given
        Properties props = new Properties();
        float expected = anyFloat();
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        Float actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Float);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeDouble() {
        // Given
        Properties props = new Properties();
        double expected = anyDouble();
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        Double actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Double);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeInt() {
        // Given
        Properties props = new Properties();
        int expected = anyInt();
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        int actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Integer);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeLong() {
        // Given
        Properties props = new Properties();
        long expected = anyLong();
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        Long actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Long);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeShort() {
        // Given
        Properties props = new Properties();
        short expected = anyShort();
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        short actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Short);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeBytes() {
        // Given
        Properties props = new Properties();
        Bytes expected = Bytes.wrap(new byte[]{anyByte()});
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        Bytes actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Bytes);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeUuid() {
        // Given
        Properties props = new Properties();
        UUID expected = UUID.fromString("f90ae889-2866-474c-b21b-88c98ea99515");
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        UUID actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.UUID);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testDeserializeKafkaAvro() throws IOException {
        // Given
        Properties props = new Properties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());

        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(ORDERS_AVRO_SCHEMA));
        IndexedRecord expected = generateOrderGenericRecord(schema, 1);

        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        GenericRecord actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Avro);

        // Then
        assertTrue(hasSchema(expectedBytes));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testDeserializeKafkaJsonSchema() {
        // Given
        Properties props = new Properties();
        props.put(SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());
        props.put(JSON_VALUE_TYPE, OrderRecord.class.getName());

        OrderRecord expected = new OrderRecord("test", "test", "test", "test");
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        OrderRecord actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.JsonSchema);

        // Then
        assertTrue(hasSchema(expectedBytes));
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testDeserializeKafkaJson() {
        // Given
        Properties props = new Properties();
        props.put(JSON_VALUE_TYPE, OrderRecord.class.getName());


        OrderRecord expected = new OrderRecord("test", "test", "test", "test");
        Headers headers = new RecordHeaders();
        byte[] expectedBytes = confluentSerializer(props, expected, headers);

        // When
        OrderRecord actual = confluentDeserializer(props, expectedBytes, headers, SerializationTypes.Json);

        // Then
        Assertions.assertFalse(hasSchema(expectedBytes));
        assertEquals(expected.toString(), actual.toString());
    }

    private static boolean hasSchema(final byte[] bytes) {
        if (bytes[0] != 0) {
            return false;
        }

        final ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, 1, 4);
        final int schemaId = byteBuffer.getInt();
        try {
            return SRUtils.getSRClient().getSchemaById(schemaId) != null;
        } catch (IOException | RestClientException e) {
            return false;
        }
    }

    private static <T> T confluentDeserializer(Properties props, byte[] expected, Headers headers, SerializationTypes serializationType) {
        try (ConfluentDeserializer confluentDeserializer = new ConfluentDeserializer()) {
            confluentDeserializer.configure(Maps.fromProperties(props), false);

            Assertions.assertNotNull(headers.headers(SerializationTypes.VALUE_HEADER_KEY));
            Assertions.assertEquals(serializationType.name(), new String(headers.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

            return (T) confluentDeserializer.deserialize(TEST_TOPIC_NAME, headers, expected);
        }
    }

    private static <T> byte[] confluentSerializer(Properties props, T expected, Headers headers) {
        try (ConfluentSerializer confluentSerializer = new ConfluentSerializer()) {
            confluentSerializer.configure(Maps.fromProperties(props), false);
            return confluentSerializer.serialize(TEST_TOPIC_NAME, headers, expected);
        }
    }

}