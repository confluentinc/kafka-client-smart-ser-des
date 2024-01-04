/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package confluent.client.integration;

import io.confluent.csid.common.test.utils.RCSUtils;
import io.confluent.csid.common.test.utils.SRUtils;
import io.confluent.csid.common.test.utils.containers.KafkaCluster;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.Tailer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.*;
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
        String config = RCSUtils.getResourceAsString("/connect-standalone.properties", ConnectToConnectIT.class);
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
        final Path schema = RCSUtils.getResourcePath("/emp.avsc", ConnectToConnectIT.class);
        String config = RCSUtils.getResourceAsString("/" + srcFilename, ConnectToConnectIT.class);
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
        return startListener(10);
    }

    protected SinkTailListener startListener(int maxLines) {
        SinkTailListener listener = new SinkTailListener(maxLines);
        Tailer tailer = new Tailer(new File("/tmp/test.sink.txt"), listener, 1000);
        Thread thread = new Thread(tailer);
        thread.setDaemon(true); // optional
        thread.start();

        return listener;
    }

    protected void startConnect() {
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
    }

    protected <T> List<T> consume(Properties properties) {
        ArrayList<T> values = new ArrayList<>();

        try (KafkaConsumer<String, T> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(java.util.Collections.singletonList("datagen_clear"));

            final int timeout = Calendar.getInstance().get(Calendar.SECOND) + 10;
            int count = 0;
            do {
                final ConsumerRecords<String, T> records = consumer.poll(java.time.Duration.ofSeconds(10));
                records.forEach(record -> {
                    T value = record.value();
                    Assertions.assertNotNull(value);

                    values.add(value);
                });

                count += records.count();
            } while (count < 10 && Calendar.getInstance().get(Calendar.SECOND) < timeout);

            Assertions.assertEquals(10, count);

            return values;
        }
    }

    public interface RecordSupplier<T> {
        T get(int count);
    }

    protected <T> void produce(Properties properties, RecordSupplier<T> recordSupplier) {
        produce(properties, recordSupplier, 10);
    }

    protected <T> void produce(Properties properties, RecordSupplier<T> recordSupplier, int maxCount) {
        int count = 0;
        try (KafkaProducer<String, T> producer = new KafkaProducer<>(properties)) {
            while (count < maxCount) {
                count++;
                producer.send(new ProducerRecord<>("datagen_clear", "key", recordSupplier.get(count)));
            }

            producer.flush();
        }
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
