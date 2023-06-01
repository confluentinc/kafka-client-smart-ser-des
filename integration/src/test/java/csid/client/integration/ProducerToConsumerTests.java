package csid.client.integration;

import csid.client.deserializer.ConfluentDeserializer;
import csid.client.integration.model.Employee;
import csid.client.integration.model.EmployeeAvro;
import csid.client.integration.model.EmployeeProto;
import csid.client.serializer.ConfluentSerializer;
import io.confluent.csid.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE;

@Testcontainers
@Slf4j
@DisplayName("Producer to Consumer Tests")
public class ProducerToConsumerTests extends TestHardening {

    @Test
    public void testJson() throws IOException, RestClientException {
        produce(getProducerProperties(), idx -> new Employee(
                String.format("emp_00%d", idx),
                String.format("user_00%d", idx),
                20 + idx,
                idx,
                String.format("user_00%d@mycompany.com", idx)));

        final List<Map<String, Object>> employees = consume(getConsumerProperties());
        Assertions.assertEquals(10, employees.size());
        employees.forEach(employee -> {
            Assertions.assertTrue(employee.containsKey("ID"));
            Assertions.assertTrue(employee.containsKey("Name"));
            Assertions.assertTrue(employee.containsKey("SSN"));
            Assertions.assertTrue(employee.containsKey("Age"));
            Assertions.assertTrue(employee.containsKey("Email"));
        });

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Test
    public void testJsonPojo() throws IOException, RestClientException {
        produce(getProducerProperties(), idx -> new Employee(
                String.format("emp_00%d", idx),
                String.format("user_00%d", idx),
                20 + idx,
                idx,
                String.format("user_00%d@mycompany.com", idx)));

        Properties properties = getConsumerProperties();
        properties.put(JSON_VALUE_TYPE, Employee.class.getName());

        final List<Employee> employees = consume(properties);
        Assertions.assertEquals(10, employees.size());
        employees.forEach(employee -> Assertions.assertTrue(employee.isValid()));

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Test
    public void testAvro() throws IOException, RestClientException {
        produce(getProducerProperties(), idx -> EmployeeAvro.newBuilder()
                .setID(String.format("emp_00%d", idx))
                .setName(String.format("user_00%d", idx))
                .setAge(20 + idx)
                .setSSN(idx)
                .setEmail(String.format("user_00%d@mycompany.com", idx))
                .build());

        final List<EmployeeAvro> employees = consume(getConsumerProperties());
        Assertions.assertEquals(10, employees.size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("AVRO", schema.schemaType());
    }

    @Test
    public void testProtobuf() throws IOException, RestClientException {

        produce(getProducerProperties(), idx -> EmployeeProto.Employee.newBuilder()
                .setID(String.format("emp_00%d", idx))
                .setName(String.format("user_00%d", idx))
                .setAge(20 + idx)
                .setSSN(idx)
                .setEmail(String.format("user_00%d@mycompany.com", idx))
                .build());

        final List<EmployeeProto.Employee> employees = consume(getConsumerProperties());
        Assertions.assertEquals(10, employees.size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("PROTOBUF", schema.schemaType());
    }


    @Override
    protected String[] getConnectArgs() {
        return new String[]{};
    }

    private Properties getProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ConfluentSerializer.class.getName());
        properties.setProperty("schema.registry.url", SRUtils.getSRClientURL());

        return properties;
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
