/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.integration;


import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import kafka.client.smart.deserializer.ConfluentDeserializer;
import kafka.client.smart.integration.model.Employee;
import kafka.client.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.*;

import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;

@Testcontainers
@Slf4j
@DisplayName("Connect to Consumer Tests")
public class ConnectToConsumerIT extends TestHardening {

    @Test
    public void testAvro() throws IOException, RestClientException {

        createConnectorConfig("connector.AVRO.properties", "connector.properties");

        startConnect();

        List<GenericRecord> objects = consume(getConsumerProperties());
        Assertions.assertEquals(10, objects.size());

        objects.forEach(genericRecord -> {
            Assertions.assertTrue(genericRecord.hasField("ID"));
            Assertions.assertTrue(genericRecord.hasField("Name"));
            Assertions.assertTrue(genericRecord.hasField("SSN"));
            Assertions.assertTrue(genericRecord.hasField("Age"));
            Assertions.assertTrue(genericRecord.hasField("Email"));
        });

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("AVRO", schema.schemaType());
    }

    @Test
    public void testProtobuf() throws IOException, RestClientException {

        createConnectorConfig("connector.PROTOBUF.properties", "connector.properties");

        startConnect();

        List<Message> objects = consume(getConsumerProperties());
        Assertions.assertEquals(10, objects.size());

        objects.forEach(message -> {
            final Map<Descriptors.FieldDescriptor, Object> fields = message.getAllFields();
            Assertions.assertTrue(fields.keySet().stream().anyMatch(fieldDescriptor -> fieldDescriptor.getName().equals("ID")));
            Assertions.assertTrue(fields.keySet().stream().anyMatch(fieldDescriptor -> fieldDescriptor.getName().equals("Name")));
            Assertions.assertTrue(fields.keySet().stream().anyMatch(fieldDescriptor -> fieldDescriptor.getName().equals("SSN")));
            Assertions.assertTrue(fields.keySet().stream().anyMatch(fieldDescriptor -> fieldDescriptor.getName().equals("Age")));
            Assertions.assertTrue(fields.keySet().stream().anyMatch(fieldDescriptor -> fieldDescriptor.getName().equals("Email")));
        });

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("PROTOBUF", schema.schemaType());
    }

    @Test
    public void testJSON() throws IOException, RestClientException {

        createConnectorConfig("connector.JSON.properties", "connector.properties");

        startConnect();

        List<Map<String, Object>> objects = consume(getConsumerProperties());
        Assertions.assertEquals(10, objects.size());

        objects.forEach(object -> {
            Assertions.assertTrue(object.containsKey("ID"));
            Assertions.assertTrue(object.containsKey("Name"));
            Assertions.assertTrue(object.containsKey("SSN"));
            Assertions.assertTrue(object.containsKey("Age"));
            Assertions.assertTrue(object.containsKey("Email"));
        });

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Test
    public void testJSONPojo() throws IOException, RestClientException {

        createConnectorConfig("connector.JSON.properties", "connector.properties");

        startConnect();

        Properties properties = getConsumerProperties();
        properties.put(JSON_VALUE_TYPE, Employee.class.getName());

        List<Employee> objects = consume(properties);
        Assertions.assertEquals(10, objects.size());
        objects.forEach(employee -> Assertions.assertTrue(employee.isValid()));

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Override
    protected String[] getConnectArgs() {
        return new String[]{
                "/tmp/connect-standalone.properties",
                "/tmp/connector.properties"
        };
    }

    private Properties getConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ConfluentDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty("schema.registry.url", SRUtils.getSRClientURL());

        return properties;
    }
}
