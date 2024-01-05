/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package confluent.client.integration;

import io.confluent.common.test.utils.SRUtils;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;

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
        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);

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
