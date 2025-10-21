/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.integration;

import kafka.client.smart.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Testcontainers
@Slf4j
@DisplayName("Connect to Connect Tests")
public class ConnectToConnectIT extends TestHardening {

    @ParameterizedTest
    @ValueSource(strings = {"AVRO", "JSON", "PROTOBUF"})
    public void testNoneDefault(String type) throws InterruptedException, IOException, RestClientException {

        createConnectorConfig("connector." + type + ".properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener();
        listener.getLatch().await(300, java.util.concurrent.TimeUnit.SECONDS);

        // Debug: Print Connect logs
        try {
            Path logPath = Paths.get("/tmp/connect.log");
            if (Files.exists(logPath)) {
                System.out.println("=== Connect Logs ===");
                Files.lines(logPath).forEach(System.out::println);
                System.out.println("=== End Connect Logs ===");
            } else {
                System.out.println("Connect log file not found at /tmp/connect.log");
            }
        } catch (Exception e) {
            System.out.println("Jeff Error reading Connect logs: " + e.getMessage());
        }

        Assertions.assertEquals(10, listener.getLines().size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals(type, schema.schemaType());
    }


    @Override
    protected String[] getConnectArgs() {
        return new String[]{
                "/tmp/connect-standalone.properties",
                "/tmp/connector.properties",
                "/tmp/connector_sink.properties"
        };
    }
}
