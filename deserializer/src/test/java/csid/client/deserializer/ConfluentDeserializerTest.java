package csid.client.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import csid.client.deserializer.record.OrderRecord;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    public final String TEST_URL = "mock://test";

    public static final int SHORT_TYPE_LENGTH = 2;
    public static final int FLOAT_TYPE_LENGTH = 4;
    public static final int INT_TYPE_LENGTH = 4;
    public static final int DOUBLE_TYPE_LENGTH = 8;
    public static final int LONG_TYPE_LENGTH = 8;
    public static final int BYTEBUFFER_CAPACITY = 10;

    public ObjectMapper MAPPER = new ObjectMapper();

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
        Object actual = confluentDeserializer(props, false, expected, byte[].class);

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
    public void testDeserializeKafkaAvro() throws IOException, RestClientException {
        // Given
        Properties props = new Properties();
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry, ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, TEST_URL));
        Schema schema = new Schema.Parser().parse(getClass().getClassLoader().getResourceAsStream(ORDERS_AVRO_SCHEMA));

        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, TEST_URL);

        IndexedRecord expected = generateOrderGenericRecord(schema, 1);
        byte[] bytes = avroSerializer.serialize(TEST_TOPIC_NAME, expected);
        schemaRegistry.register("test-subject", new AvroSchema(expected.getSchema()));

        // When
        Object actual = confluentDeserializer(props, false, bytes, IndexedRecord.class, schemaRegistry);

        // Then
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testDeserializeKafkaJsonSchema() throws RestClientException, IOException {
        // Given
        String schema = "{\n"
                + "  \"$schema\": \"http://json-schema.org/draft-07/schema#\",\n"
                + "  \"title\": \"Schema references\",\n"
                + "  \"description\": \"List of schema references for multiple types in a single topic\",\n"
                + "  \"oneOf\": [\n"
                + "    { \"$ref\": \"order.json\"}\n"
                + "  ]\n"
                + "}";

        Properties props = new Properties();
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        KafkaJsonSchemaSerializer jsonSchemaSerializer = new KafkaJsonSchemaSerializer(schemaRegistry, ImmutableMap.of(SCHEMA_REGISTRY_URL_CONFIG, TEST_URL));


        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, TEST_URL);

        OrderRecord expected = new OrderRecord("test", "test", "test", "test");

        JsonSchema userSchema = JsonSchemaUtils.getSchema(expected);
        schemaRegistry.register("order", userSchema);
        List<SchemaReference> refs = ImmutableList.of(
                new io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference(
                        "order.json", "order", 1));
        Map<String, String> resolvedRefs = ImmutableMap.of("order.json", userSchema.canonicalString());
        JsonSchema jsonSchema = new JsonSchema(schema, refs, resolvedRefs, null);
        schemaRegistry.register(TEST_TOPIC_NAME, jsonSchema);

        byte[] bytes = jsonSchemaSerializer.serialize(TEST_TOPIC_NAME, expected);

        // When
        Object actualObject = confluentDeserializer(props, false, bytes, OrderRecord.class, schemaRegistry);
        OrderRecord actual = MAPPER.convertValue(actualObject, OrderRecord.class);

        // Then
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testDeserializeKafkaJson() {
        // Given
        Properties props = new Properties();


        KafkaJsonSerializer<Object> serializer = new KafkaJsonSerializer<>();
        Map<String, Object> config = new HashMap<>();
        serializer.configure(config, false);
        OrderRecord expected = new OrderRecord("test", "test", "test", "test");
        byte[] bytes = serializer.serialize(TEST_TOPIC_NAME, expected);

        // When
        Object deserializeActual = confluentDeserializer(props, false, bytes, OrderRecord.class);
        OrderRecord actual = MAPPER.convertValue(deserializeActual, OrderRecord.class);

        // Then
        assertEquals(expected, actual);
    }

    private static Object confluentDeserializer(Properties props, boolean isKey, byte[] expected, Class<?> clazz) {
        try (ConfluentDeserializer<?> confluentDeserializer = new ConfluentDeserializer<>(props, isKey, clazz)) {
            return confluentDeserializer.deserialize(TEST_TOPIC_NAME, expected);
        }
    }

    private static Object confluentDeserializer(Properties props, boolean isKey, byte[] expected, Class<?> clazz, SchemaRegistryClient schemaRegistryClient) {
        try (ConfluentDeserializer<?> confluentDeserializer = new ConfluentDeserializer<>(props, isKey, clazz, schemaRegistryClient)) {
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