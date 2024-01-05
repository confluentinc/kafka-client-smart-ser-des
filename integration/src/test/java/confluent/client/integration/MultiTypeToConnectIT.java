/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package confluent.client.integration;

import confluent.client.integration.model.Employee;
import confluent.client.serializer.ConfluentSerializer;
import io.confluent.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class MultiTypeToConnectIT extends TestHardening {

    @Test
    public void testJson() throws IOException, RestClientException, InterruptedException {
        final Employee expectedEmployee = new Employee(
                "emp_001",
                "user_001",
                20,
                1,
                "user_001@mycompany.com");

        createConnectorConfig("connector.JSON.properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener(2);

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(getProducerProperties())) {
            producer.send(new ProducerRecord<>("datagen_clear", "key1", expectedEmployee));
            producer.send(new ProducerRecord<>("datagen_clear", "key2", 123));
            producer.flush();
        }

        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertEquals(2, listener.getLines().size());
        Assertions.assertEquals(expectedEmployee, new Employee((Map<String, String>) listener.getLines().get(0)));
        Assertions.assertEquals(123, Integer.parseInt(listener.getLines().get(1).toString()));

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Override
    protected String[] getConnectArgs() {
        return new String[]{
                "/tmp/connect-standalone.properties",
                "/tmp/connector_sink.properties"
        };
    }

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfluentSerializer.class.getName());
        properties.setProperty("schema.registry.url", SRUtils.getSRClientURL());

        return properties;
    }
}
