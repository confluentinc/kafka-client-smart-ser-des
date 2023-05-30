package csid.client.integration;

import csid.client.integration.model.Employee;
import csid.client.integration.model.EmployeeAvro;
import csid.client.integration.model.EmployeeProto;
import csid.client.serializer.ConfluentSerializer;
import io.confluent.csid.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Properties;

@Testcontainers
@Slf4j
@DisplayName("Producer to Connect Tests")
public class ProducerToConnectTests extends TestHardening {

    @Test
    public void testJson() throws IOException, RestClientException, InterruptedException {

        createConnectorConfig("connector.JSON.properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener();

        // create the producer
        try (KafkaProducer<String, Employee> producer = new KafkaProducer<>(getProducerProperties())) {
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_001", "user_001", 21, 1, "aaaa@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_002", "user_002", 22, 2, "bbbb@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_003", "user_003", 23, 3, "cccc@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_004", "user_004", 24, 4, "dddd@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_005", "user_005", 25, 5, "eeee@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_006", "user_006", 26, 6, "ffff@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_007", "user_007", 27, 7, "gggg@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_008", "user_008", 28, 8, "hhhh@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_009", "user_009", 29, 9, "iiii@mycompany.com")));
            producer.send(new ProducerRecord<>("datagen_clear", "key", new Employee("emp_010", "user_010", 30, 10, "jjjj@mycompany.com")));
            producer.flush();
        }

        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertEquals(10, listener.getLines().size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("JSON", schema.schemaType());
    }

    @Test
    public void testAvro() throws IOException, RestClientException, InterruptedException {

        createConnectorConfig("connector.AVRO.properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener();

        // create the producer
        try (KafkaProducer<String, EmployeeAvro> producer = new KafkaProducer<>(getProducerProperties())) {
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_001").setName("user_001").setAge(21).setSSN(1).setEmail("aaaa@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_002").setName("user_002").setAge(22).setSSN(2).setEmail("bbbb@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_003").setName("user_003").setAge(23).setSSN(3).setEmail("cccc@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_004").setName("user_004").setAge(24).setSSN(4).setEmail("dddd@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_005").setName("user_005").setAge(25).setSSN(5).setEmail("eeee@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_006").setName("user_006").setAge(26).setSSN(6).setEmail("ffff@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_007").setName("user_007").setAge(27).setSSN(7).setEmail("gggg@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_008").setName("user_008").setAge(28).setSSN(8).setEmail("hhhh@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_009").setName("user_009").setAge(29).setSSN(9).setEmail("iiiii@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeAvro.newBuilder().setID("emp_010").setName("user_010").setAge(30).setSSN(10).setEmail("jjjj@mycompany.com").build()));

            producer.flush();
        }

        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertEquals(10, listener.getLines().size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("AVRO", schema.schemaType());
    }

    @Test
    public void testProtobuf() throws IOException, RestClientException, InterruptedException {

        createConnectorConfig("connector.PROTOBUF.properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener();

        // create the producer
        try (KafkaProducer<String, EmployeeProto.Employee> producer = new KafkaProducer<>(getProducerProperties())) {
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_001").setName("user_001").setAge(21).setSSN(1).setEmail("aaaa@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_002").setName("user_002").setAge(22).setSSN(2).setEmail("bbbb@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_003").setName("user_003").setAge(23).setSSN(3).setEmail("cccc@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_004").setName("user_004").setAge(24).setSSN(4).setEmail("dddd@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_005").setName("user_005").setAge(25).setSSN(5).setEmail("eeee@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_006").setName("user_006").setAge(26).setSSN(6).setEmail("ffff@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_007").setName("user_007").setAge(27).setSSN(7).setEmail("gggg@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_008").setName("user_008").setAge(28).setSSN(8).setEmail("hhhh@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_009").setName("user_009").setAge(29).setSSN(9).setEmail("iiiii@mycompany.com").build()));
            producer.send(new ProducerRecord<>("datagen_clear", "key", EmployeeProto.Employee.newBuilder().setID("emp_010").setName("user_010").setAge(30).setSSN(10).setEmail("jjjj@mycompany.com").build()));

            producer.flush();
        }

        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);
        Assertions.assertEquals(10, listener.getLines().size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("PROTOBUF", schema.schemaType());
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
