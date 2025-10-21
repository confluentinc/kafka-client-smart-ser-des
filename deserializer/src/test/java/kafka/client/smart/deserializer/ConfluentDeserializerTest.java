/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import kafka.client.smart.deserializer.record.OrderRecord;
import kafka.client.smart.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;

public class ConfluentDeserializerTest {

    public static final String TEST_TOPIC_NAME = "test-deserializer-topic";

    public static final String ORDERS_AVRO_SCHEMA = "schema/orders-avro.avsc";

    public static final int SHORT_TYPE_LENGTH = 2;
    public static final int FLOAT_TYPE_LENGTH = 4;
    public static final int INT_TYPE_LENGTH = 4;
    public static final int DOUBLE_TYPE_LENGTH = 8;
    public static final int LONG_TYPE_LENGTH = 8;
    public static final int BYTEBUFFER_CAPACITY = 10;

    public ObjectMapper MAPPER = new ObjectMapper();

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {String.class})
    public void testDeserializeString(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        String expected = anyString();
        byte[] expectedBytes = expected.getBytes(StandardCharsets.UTF_8);

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {Boolean.class})
    public void testDeserializeBoolean(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        boolean expectedBoolean = anyBoolean();
        byte[] expectedBytes = new byte[]{(byte) (expectedBoolean ? 1 : 0)};

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, Boolean.class);

        // Then
        assertEquals(expectedBoolean, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {byte[].class})
    public void testDeserializeByteArray(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};

        // When
        Object actual = confluentDeserializer(props, false, expected, clazz);

        // Then
        if (clazz == null) {
            // When clazz is null, type is detected from bytes
            // Single byte [0] or [1] is detected as Boolean
            if (expected.length == 1 && (expected[0] == 0x00 || expected[0] == 0x01)) {
                assertEquals(expected[0] == 0x01, actual);
            } else {
                assertEquals(expected, actual);
            }
        } else {
            // When clazz is byte[].class, expect the original byte array
            assertEquals(expected, actual);
        }
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {ByteBuffer.class})
    public void testDeserializeByteBuffer(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        ByteBuffer expected = ByteBuffer.allocate(BYTEBUFFER_CAPACITY);
        byte[] expectedBytes = expected.array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);

    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {Float.class})
    public void testDeserializeFloat(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        float expected = anyFloat();
        byte[] expectedBytes = ByteBuffer.allocate(FLOAT_TYPE_LENGTH).putFloat(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {Double.class})
    public void testDeserializeDouble(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        double expected = anyDouble();
        byte[] expectedBytes = ByteBuffer.allocate(DOUBLE_TYPE_LENGTH).putDouble(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {Integer.class})
    public void testDeserializeInt(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        int expected = anyInt();
        byte[] expectedBytes = ByteBuffer.allocate(INT_TYPE_LENGTH).putInt(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {Long.class})
    public void testDeserializeLong(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        long expected = anyLong();
        byte[] expectedBytes = ByteBuffer.allocate(LONG_TYPE_LENGTH).putLong(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {Short.class})
    public void testDeserializeShort(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        short expected = anyShort();
        byte[] expectedBytes = ByteBuffer.allocate(SHORT_TYPE_LENGTH).putShort(expected).array();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {Bytes.class})
    public void testDeserializeBytes(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        Bytes expected = Bytes.wrap(new byte[]{anyByte()});
        byte[] expectedBytes = expected.get();

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @ValueSource(classes = {UUID.class})
    public void testDeserializeUuid(Class<?> clazz) {
        // Given
        Properties props = new Properties();
        UUID expected = UUID.fromString("f90ae889-2866-474c-b21b-88c98ea99515");
        byte[] expectedBytes = expected.toString().getBytes(StandardCharsets.UTF_8);

        // When
        Object actual = confluentDeserializer(props, false, expectedBytes, clazz);

        // Then
        assertEquals(expected, actual);
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {IndexedRecord.class})
    public void testDeserializeKafkaAvro(Class<?> clazz) throws IOException, RestClientException {
        // Given
        Properties props = new Properties();
        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());

        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(ORDERS_AVRO_SCHEMA));
        IndexedRecord expected = generateOrderGenericRecord(schema, 1);

        byte[] bytes;
        try (KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer()) {
            avroSerializer.configure(Maps.fromProperties(props), false);
            bytes = avroSerializer.serialize(TEST_TOPIC_NAME, expected);
        }

        // When
        Object actual = confluentDeserializer(props, false, bytes, clazz);

        // Then
        assertEquals(expected.toString(), actual.toString());
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {OrderRecord.class})
    public void testDeserializeKafkaJsonSchema(Class<?> clazz) throws RestClientException, IOException {
        // Given
        Properties props = new Properties();
        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());
        props.setProperty(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicRecordNameStrategy.class.getName());
        props.setProperty(JSON_VALUE_TYPE, OrderRecord.class.getName());

        OrderRecord expected = new OrderRecord("test", "test", "test", "test");
        byte[] bytes;
        try (KafkaJsonSchemaSerializer<OrderRecord> jsonSchemaSerializer = new KafkaJsonSchemaSerializer<>()) {
            jsonSchemaSerializer.configure(Maps.fromProperties(props), false);

            bytes = jsonSchemaSerializer.serialize(TEST_TOPIC_NAME, expected);
        }

        // When
        OrderRecord actual = (OrderRecord) confluentDeserializer(props, false, bytes, clazz);

        // Then
        assertEquals(expected.toString(), actual.toString());
    }

    @ParameterizedTest(name = "#{index} - Run test with args={0}")
    @NullSource
    @ValueSource(classes = {OrderRecord.class})
    public void testDeserializeKafkaJson(Class<OrderRecord> clazz) {
        // Given
        Properties props = new Properties();


        KafkaJsonSerializer<Object> serializer = new KafkaJsonSerializer<>();
        Map<String, Object> config = new HashMap<>();
        serializer.configure(config, false);
        OrderRecord expected = new OrderRecord("test", "test", "test", "test");
        byte[] bytes = serializer.serialize(TEST_TOPIC_NAME, expected);

        // When
        Object deserializeActual = confluentDeserializer(props, false, bytes, clazz);
        OrderRecord actual = MAPPER.convertValue(deserializeActual, OrderRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    private static Object confluentDeserializer(Properties props, boolean isKey, byte[] expected, Class<?> clazz) {
        try (ConfluentDeserializer confluentDeserializer = new ConfluentDeserializer(clazz)) {
            confluentDeserializer.configure(Maps.fromProperties(props), isKey);
            return confluentDeserializer.deserialize(TEST_TOPIC_NAME, expected);
        }
    }

    public static GenericRecord generateOrderGenericRecord(Schema schema, Integer numberOfEvents) {
        GenericRecord payload = new GenericData.Record(schema);
        payload.put("name", "name" + numberOfEvents);
        payload.put("orderRef", "orderRef" + numberOfEvents);
        payload.put("customer", "customer" + numberOfEvents);
        payload.put("customerCode", "Code" + numberOfEvents);
        return payload;
    }


}
