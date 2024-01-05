/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package csid.client.serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import csid.client.common.SerializationTypes;
import csid.client.common.schema.SchemaRegistryUtils;
import csid.client.serializer.record.*;
import io.confluent.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;

class ConfluentSerializerTest {

    private static final String TEST_TOPIC_NAME = "test-serializer-topic";
    private static final byte MAGIC_BYTE = 0x0;
    private final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testSerializeString() {
        // Given
        Properties props = new Properties();
        String expected = anyString();

        // When
        byte[] actual = confluentSerializer(props, false, expected, SerializationTypes.String);

        // Then
        assertEquals(expected, new String(actual));
    }

    @Test
    public void testSerializeByteArray() {
        // Given
        Properties props = new Properties();
        byte[] expected = {anyByte()};

        // When
        byte[] actual = confluentSerializer(props, false, expected, SerializationTypes.ByteArray);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeShort() {
        // Given
        Properties props = new Properties();
        short expected = anyShort();

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Short);
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
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Integer);
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
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Long);
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
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Float);
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
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Double);

        // Then
        ByteBuffer actual = ByteBuffer.wrap(actualBytes);

        assertEquals(expected, actual.getDouble());
    }

    @Test
    public void testSerializeBytes() {
        // Given
        Properties props = new Properties();
        byte[] byteArray = {anyByte()};
        Bytes expected = Bytes.wrap(byteArray);

        // When
        byte[] actual = confluentSerializer(props, false, expected, SerializationTypes.Bytes);

        // Then
        assertEquals(expected.get(), actual);
    }

    @Test
    public void testSerializeUuid() {
        // Given
        Properties props = new Properties();
        UUID expected = UUID.fromString("f90ae889-2866-474c-b21b-88c98ea99515");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.UUID);
        UUID actual = UUID.fromString(new String(actualBytes));

        // Then
        assertEquals(expected, actual);
    }


    @Test
    public void testSerializeKafkaJsonSchema() throws IOException {
        SRUtils.reset();

        // Given
        Properties props = new Properties();
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());
        OrderSchemaRecord expected = new OrderSchemaRecord("test", "test", "test", "test");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.JsonSchema);
        final ByteBuffer byteBuffer = ByteBuffer.wrap(actualBytes);

        Assertions.assertEquals(MAGIC_BYTE, byteBuffer.get());
        Assertions.assertEquals(1, byteBuffer.getInt());

        byte[] jsonBytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(jsonBytes);
        OrderSchemaRecord actual = MAPPER.readValue(jsonBytes, OrderSchemaRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testSerializeKafkaJson() throws IOException {
        // Given
        Properties props = new Properties();
        OrderRecord expected = new OrderRecord("test", "test", "test", "test");

        // When
        byte[] actualBytes = confluentSerializer(props, false, expected, SerializationTypes.Json);
        OrderRecord actual = MAPPER.readValue(actualBytes, OrderRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    @Test
    public void testPrimarySerialization() throws RestClientException, IOException {
        ConfluentSerializer serializer = getSerializer(new HashMap<String, String>() {{
            put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SRUtils.getSRClientURL());
        }}, false);

        // Given
        String stringExpected = "Sample String";
        byte[] bytesExpected = stringExpected.getBytes();
        int intExpected = 123;
        Employee employeeJsonExpected = new Employee("John", "Doe", 25, 1, "john@doe.com");
        EmployeeProto.Employee employeeProtoExpected = EmployeeProto.Employee.newBuilder()
                .setID("John")
                .setName("Doe")
                .setAge(25)
                .setSSN(1)
                .setEmail("john@doe.com")
                .build();
        EmployeeAvro employeeAvroExpected = EmployeeAvro.newBuilder()
                .setID("John")
                .setName("Doe")
                .setAge(25)
                .setSSN(1)
                .setEmail("john@doe.com")
                .build();
        OrderRecord orderJsonExpected = new OrderRecord("test", "test", "test", "test");

        // When
        Headers stringHeaders = new RecordHeaders();
        byte[] stringActual = serializer.serialize(TEST_TOPIC_NAME, stringHeaders, stringExpected);
        Assertions.assertNotNull(stringHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.String.name(), new String(stringHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));


        Headers bytesHeaders = new RecordHeaders();
        byte[] bytesActual = serializer.serialize(TEST_TOPIC_NAME, bytesHeaders, bytesExpected);
        Assertions.assertNotNull(bytesHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.ByteArray.name(), new String(bytesHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        Headers intHeaders = new RecordHeaders();
        byte[] intActual = serializer.serialize(TEST_TOPIC_NAME, intHeaders, intExpected);
        Assertions.assertNotNull(intHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.Integer.name(), new String(intHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        Headers jsonSchemaHeaders = new RecordHeaders();
        byte[] jsonSchemaActual = serializer.serialize(TEST_TOPIC_NAME, jsonSchemaHeaders, employeeJsonExpected);
        Assertions.assertNotNull(jsonSchemaHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.JsonSchema.name(), new String(jsonSchemaHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        Headers avroSchemaHeaders = new RecordHeaders();
        byte[] avroSchemaActual = serializer.serialize(TEST_TOPIC_NAME, avroSchemaHeaders, employeeAvroExpected);
        Assertions.assertNotNull(avroSchemaHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.Avro.name(), new String(avroSchemaHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        Headers protoSchemaHeaders = new RecordHeaders();
        byte[] protoSchemaActual = serializer.serialize(TEST_TOPIC_NAME, protoSchemaHeaders, employeeProtoExpected);
        Assertions.assertNotNull(protoSchemaHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.Protobuf.name(), new String(protoSchemaHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        Headers orderJsonSchemaHeaders = new RecordHeaders();
        byte[] orderJsonSchemaActual = serializer.serialize(TEST_TOPIC_NAME, orderJsonSchemaHeaders, orderJsonExpected);
        Assertions.assertNotNull(orderJsonSchemaHeaders.headers(SerializationTypes.VALUE_HEADER_KEY));
        Assertions.assertEquals(SerializationTypes.JsonSchema.name(), new String(orderJsonSchemaHeaders.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));

        // Then
        Assertions.assertEquals("AVRO", SchemaRegistryUtils.getSchemaType(SRUtils::getSRClient, avroSchemaActual));
        Assertions.assertEquals("PROTOBUF", SchemaRegistryUtils.getSchemaType(SRUtils::getSRClient, protoSchemaActual));
        Assertions.assertEquals("JSON", SchemaRegistryUtils.getSchemaType(SRUtils::getSRClient, jsonSchemaActual));
        Assertions.assertEquals("JSON", SchemaRegistryUtils.getSchemaType(SRUtils::getSRClient, orderJsonSchemaActual));
        Assertions.assertEquals(stringExpected, new String(stringActual));
        Assertions.assertEquals(bytesExpected, bytesActual);
        Assertions.assertEquals(intExpected, ByteBuffer.wrap(intActual).getInt());
    }


    private static ConfluentSerializer getSerializer(Map<String, ?> props, boolean isKey) {
        ConfluentSerializer confluentSerializer = new ConfluentSerializer();
        confluentSerializer.configure(props, isKey);

        return confluentSerializer;
    }

    private static <T> byte[] confluentSerializer(Properties props, boolean isKey, T expected, SerializationTypes serializationType) {
        try (ConfluentSerializer confluentSerializer = new ConfluentSerializer()) {
            confluentSerializer.configure(Maps.fromProperties(props), isKey);

            final Headers headers = new RecordHeaders();
            final byte[] serialized = confluentSerializer.serialize(TEST_TOPIC_NAME, headers, expected);
            Assertions.assertNotNull(headers.headers(SerializationTypes.VALUE_HEADER_KEY));
            Assertions.assertEquals(serializationType.name(), new String(headers.lastHeader(SerializationTypes.VALUE_HEADER_KEY).value()));
            return serialized;
        }
    }
}