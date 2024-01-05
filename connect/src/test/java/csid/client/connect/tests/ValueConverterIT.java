/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package csid.client.connect.tests;

import io.confluent.common.test.utils.RCSUtils;
import io.confluent.common.test.utils.SRUtils;
import io.confluent.common.test.utils.containers.KafkaCluster;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.Tailer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Testcontainers
@Slf4j
public class ValueConverterIT {
    protected KafkaCluster cluster;
    private Connect connect;
    @Test
    public void testDefaultAVRO() throws InterruptedException, IOException, RestClientException {

        createConnectorConfig("connector.properties", "connector.properties");

        startConnect();

        SinkTailListener listener = startListener();
        listener.getLatch().await(100, java.util.concurrent.TimeUnit.SECONDS);

        Assertions.assertEquals(10, listener.getLines().size());

        final ParsedSchema schema = SRUtils.getSRClient().getSchemaById(1);
        Assertions.assertEquals("AVRO", schema.schemaType());
    }

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

    @BeforeEach
    public void setup() throws IOException {
        deleteFile("/tmp/connect-standalone.properties");
        deleteFile("/tmp/connector.properties");
        deleteFile("/tmp/connector_sink.properties");
        deleteFile("/tmp/test.sink.txt");

        cluster = KafkaCluster.defaultCluster().withReuse(true);
        cluster.start();

        SRUtils.reset();

        createConfigFile();
        createConnectorConfig("connector_sink.properties", "connector_sink.properties");
        new File("/tmp/test.sink.txt").createNewFile();
    }

    @AfterEach
    public void teardown() {
        stopConnect();
        cluster.stop();

        deleteFile("/tmp/connect-standalone.properties");
        deleteFile("/tmp/connector.properties");
        deleteFile("/tmp/connector_sink.properties");
        deleteFile("/tmp/test.sink.txt");
    }

    private void deleteFile(String filename) {
        File configFile = new File(filename);
        if (configFile.exists()) {
            configFile.delete();
        }
    }

    private void createConfigFile() throws IOException {
        String config = RCSUtils.getResourceAsString("/connect-standalone.properties", ValueConverterIT.class);
        config = config.replace("{{bootstrap.servers}}", cluster.getBootstrapServers());

        File configFile = new File("/tmp/connect-standalone.properties");
        if (configFile.exists()) {
            configFile.delete();
        }

        FileWriter myWriter = new FileWriter("/tmp/connect-standalone.properties");
        myWriter.write(config);
        myWriter.close();
    }

    private void createConnectorConfig(String srcFilename, String targetFilename) throws IOException {
        final Path schema = RCSUtils.getResourcePath("/emp.avsc", ValueConverterIT.class);
        String config = RCSUtils.getResourceAsString("/" + srcFilename, ValueConverterIT.class);
        config = config.replace("{{schema}}", schema.toString());

        FileWriter myWriter = new FileWriter("/tmp/" + targetFilename);
        myWriter.write(config);
        myWriter.close();
    }

    private void stopConnect() {
        if (connect != null) {
            connect.stop();
            connect.awaitStop();
        }
    }

    private SinkTailListener startListener() {
        SinkTailListener listener = new SinkTailListener(10);
        Tailer tailer = new Tailer(new File("/tmp/test.sink.txt"), listener, 1000);
        Thread thread = new Thread(tailer);
        thread.setDaemon(true); // optional
        thread.start();

        return listener;
    }

    private Thread startConnect() {
        final Thread connectorThread = new Thread(() -> {
            try {
                startConnectWorker(new String[] {
                        "/tmp/connect-standalone.properties",
                        "/tmp/connector.properties",
                        "/tmp/connector_sink.properties"
                });
                connect.awaitStop();
            } catch (Throwable e) {
                log.error("Error running connector", e);
            } finally {
                stopConnect();
            }
        });
        connectorThread.start();

        return connectorThread;
    }

    private void startConnectWorker(final String[] args) throws ExecutionException, InterruptedException, IOException {
        Time time = Time.SYSTEM;
        log.info("Kafka Connect standalone worker initializing ...");
        long initStart = time.hiResClockMs();
        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();

        String workerPropsFile = args[0];
        Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.emptyMap();

        log.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        StandaloneConfig config = new StandaloneConfig(workerProps);

        String kafkaClusterId = config.kafkaClusterId();
        log.debug("Kafka cluster ID: {}", kafkaClusterId);
        RestClient client = new RestClient(config);

        // Do not initialize a RestClient because the ConnectorsResource will not use it in standalone mode.
        ConnectRestServer rest = new ConnectRestServer(config.rebalanceTimeout(), client, workerProps);
        rest.initializeServer();

        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        Herder herder = createHerder(config, workerId, plugins, connectorClientConfigOverridePolicy);

        connect = new Connect(herder, rest);
        log.info("Kafka Connect standalone worker initialization took {}ms", time.hiResClockMs() - initStart);

        connect.start();
        for (final String connectorPropsFile : Arrays.copyOfRange(args, 1, args.length)) {
            Map<String, String> connectorProps = Utils.propsToStringMap(Utils.loadProps(connectorPropsFile));
            FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                if (error != null)
                    log.error("Failed to create job for {}", connectorPropsFile);
                else
                    log.info("Created connector {}", info.result().name());
            });
            herder.putConnectorConfig(
                    connectorProps.get(ConnectorConfig.NAME_CONFIG),
                    connectorProps, false, cb);
            cb.get();
        }
    }

    private Herder createHerder(StandaloneConfig config,
                                String workerId,
                                Plugins plugins,
                                ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy) {

        OffsetBackingStore offsetBackingStore = new FileOffsetBackingStore(plugins.newInternalConverter(
                true, JsonConverter.class.getName(), Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false")));
        offsetBackingStore.configure(config);

        Worker worker = new Worker(workerId, Time.SYSTEM, plugins, config, offsetBackingStore,
                connectorClientConfigOverridePolicy);

        return new StandaloneHerder(worker, config.kafkaClusterId(), connectorClientConfigOverridePolicy);
    }
}
