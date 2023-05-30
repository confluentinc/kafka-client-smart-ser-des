package csid.client.integration;

import io.confluent.csid.common.test.utils.RCSUtils;
import io.confluent.csid.common.test.utils.SRUtils;
import io.confluent.csid.common.test.utils.containers.KafkaCluster;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.Tailer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.*;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public abstract class TestHardening {

    protected KafkaCluster cluster;
    protected Connect connect;

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

    protected void deleteFile(String filename) {
        File configFile = new File(filename);
        if (configFile.exists()) {
            configFile.delete();
        }
    }

    protected void createConfigFile() throws IOException {
        String config = RCSUtils.getResourceAsString("/connect-standalone.properties", ConnectToConnectTests.class);
        config = config.replace("{{bootstrap.servers}}", cluster.getBootstrapServers());

        File configFile = new File("/tmp/connect-standalone.properties");
        if (configFile.exists()) {
            configFile.delete();
        }

        FileWriter myWriter = new FileWriter("/tmp/connect-standalone.properties");
        myWriter.write(config);
        myWriter.close();
    }

    protected void createConnectorConfig(String srcFilename, String targetFilename) throws IOException {
        final Path schema = RCSUtils.getResourcePath("/emp.avsc", ConnectToConnectTests.class);
        String config = RCSUtils.getResourceAsString("/" + srcFilename, ConnectToConnectTests.class);
        config = config.replace("{{schema}}", schema.toString());

        FileWriter myWriter = new FileWriter("/tmp/" + targetFilename);
        myWriter.write(config);
        myWriter.close();
    }

    protected void stopConnect() {
        if (connect != null) {
            connect.stop();
            connect.awaitStop();
        }
    }

    protected SinkTailListener startListener() {
        SinkTailListener listener = new SinkTailListener(10);
        Tailer tailer = new Tailer(new File("/tmp/test.sink.txt"), listener, 1000);
        Thread thread = new Thread(tailer);
        thread.setDaemon(true); // optional
        thread.start();

        return listener;
    }

    protected Thread startConnect() {
        final Thread connectorThread = new Thread(() -> {
            try {
                startConnectWorker(getConnectArgs());
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

    protected abstract String[] getConnectArgs();

    protected void startConnectWorker(final String[] args) throws ExecutionException, InterruptedException, IOException {
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

        // Do not initialize a RestClient because the ConnectorsResource will not use it in standalone mode.
        RestServer rest = new RestServer(config, null);
        rest.initializeServer();

        URI advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        OffsetBackingStore offsetBackingStore = new FileOffsetBackingStore();
        offsetBackingStore.configure(config);

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);
        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore,
                connectorClientConfigOverridePolicy);

        Herder herder = new StandaloneHerder(worker, kafkaClusterId, connectorClientConfigOverridePolicy);
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

}
