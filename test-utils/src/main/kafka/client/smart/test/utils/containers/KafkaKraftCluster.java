/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.common.test.utils.containers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.Uuid;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

/**
 * Containers within the network can access the brokers by using port 9092. 9093 is exposed outside
 * and is put as an advertised listener
 */
public class KafkaKraftCluster implements Startable {

  // timeout in minutes
  public static final int DEFAULT_WAIT_TIMEOUT = 2;
  private static final String DEFAULT_IMAGE_NAME = "confluentinc/cp-kafka";
  private static final DockerImageName DEFAULT_IMAGE =
      ContainerUtils.getDockerImageNameFrom(
          DEFAULT_IMAGE_NAME, ContainerUtils.DEFAULT_CP_IMAGE_TAG);
  private final Map<String, String> additionalContainerEnv = new HashMap<>();
  @Getter private final Network network;

  @Getter @Setter private final String clusterId = generateClusterId();

  @Getter @Setter private boolean reuseContainers = false;
  private boolean addSchemaRegistry = false;
  private int numBrokers;
  @Getter private SchemaRegistryContainer schemaRegistryContainer;
  private Collection<KafkaContainer> kafkaContainers = new ArrayList<>();
  private String imageName = DEFAULT_IMAGE_NAME;
  private String imageTag = ContainerUtils.DEFAULT_CP_IMAGE_TAG;
  private DockerImageName dockerImageName = DEFAULT_IMAGE;
  @Getter @Setter private int waitTimeout = DEFAULT_WAIT_TIMEOUT;

  public KafkaKraftCluster() {
    this(Network.newNetwork());
  }

  public KafkaKraftCluster(Network network) {
    this.network = network;
  }

  public static KafkaKraftCluster defaultCluster() {
    final KafkaKraftCluster cluster = KafkaKraftCluster.newCluster();
    return cluster.withNumBrokers(1);
  }

  public static KafkaKraftCluster newCluster() {
    return new KafkaKraftCluster();
  }

  public KafkaKraftCluster withNumBrokers(int numBrokers) {
    this.numBrokers = numBrokers;
    return this;
  }

  public int getClusterSize() {
    return kafkaContainers.size();
  }

  /**
   * Adds some env vars to the Kafka container.
   *
   * @param key: the env var key
   * @param value: the env var value
   * @return the current cluster with the added container envs
   */
  public KafkaKraftCluster withContainerEnv(String key, String value) {
    additionalContainerEnv.put(key, value);
    return this;
  }

  /**
   * Set how long to wait (in minutes) before timing out when starting a cluster
   *
   * @param timeout the timeout period in minutes
   * @return this instance
   */
  public KafkaKraftCluster withWaitTimeout(int timeout) {
    setWaitTimeout(timeout);
    return this;
  }

  public KafkaKraftCluster withReuseContainers(boolean reuseContainers) {
    setReuseContainers(true);
    return this;
  }

  /**
   * Set the image name and tag for the Kafka cluster. The image tag will be appended with ".arm64"
   * for ARM-based environments.
   *
   * @param imageName the image name of the Kafka cluster
   * @param imageTag the image tag of the Kafka cluster
   * @return this instance
   */
  public KafkaKraftCluster withImage(String imageName, String imageTag) {
    this.dockerImageName = ContainerUtils.getDockerImageNameFrom(imageName, imageTag);
    return this;
  }

  /**
   * Set the image name for the Kafka cluster. The image tag will be appended with ".arm64" for
   *
   * @param imageName the image name of the Kafka cluster
   * @return this instance
   */
  public KafkaKraftCluster withImageName(String imageName) {
    this.imageName = imageName;
    this.dockerImageName = ContainerUtils.getDockerImageNameFrom(this.imageName, this.imageTag);
    return this;
  }

  /**
   * Set the image tag for the Kafka cluster. The image tag will be appended with ".arm64" for
   *
   * @param imageTag the image tag of the Kafka cluster
   * @return this instance
   */
  public KafkaKraftCluster withImageTag(String imageTag) {
    this.imageTag = imageTag;
    this.dockerImageName = ContainerUtils.getDockerImageNameFrom(this.imageName, this.imageTag);
    return this;
  }

  /** @return An unmodifiable collection of Kafka containers in the cluster */
  public Collection<KafkaContainer> getKafkaContainers() {
    return Collections.unmodifiableCollection(kafkaContainers);
  }

  /** @return the container logs for all the Kafka containers in the cluster */
  public String getKafkaContainerLogs() {
    return IntStream.range(0, kafkaContainers.size())
        .mapToObj(
            i -> {
              String header = String.format("--- Kafka Container %d Logs---%n", i);
              return header + kafkaContainers.stream().skip(i).findFirst().get().getLogs();
            })
        .collect(Collectors.joining("\n"));
  }

  /** @return the container logs for the Schema Registry container */
  public String getSchemaRegistryContainerLogs() {
    if (schemaRegistryContainer == null) {
      return "";
    }
    String header = "--- Schema Registry Container Logs---\n";
    return header + schemaRegistryContainer.getLogs();
  }

  /** @return are all the Kafka containers in the cluster running? */
  public boolean isClusterRunning() {
    return kafkaContainers.stream()
        .map(KafkaContainer::isRunning)
        .reduce(true, (previous, current) -> previous && current);
  }

  /** @return Retrieve the container for the Kafka broker with the given broker ID */
  public KafkaContainer getContainer(int brokerId) {
    return kafkaContainers.stream()
        .filter(
            kafkaContainer ->
                kafkaContainer.getEnvMap().get("KAFKA_BROKER_ID").equals(String.valueOf(brokerId)))
        .findFirst()
        .orElse(null);
  }

  /**
   * Retrieve the dockerHostIpAddress for the given host port pair
   *
   * @param originalHostName the original host name
   * @param originalPort the original port
   * @return
   */
  public String getServiceHost(String originalHostName, int originalPort) {
    return kafkaContainers.stream()
        .filter(kafkaContainer -> kafkaContainer.getNetworkAliases().contains(originalHostName))
        .filter(kafkaContainer -> kafkaContainer.getMappedPort(originalPort) != null)
        .findFirst()
        .map(ContainerState::getHost)
        .orElseThrow(() -> new RuntimeException("Invalid Host"));
  }

  /**
   * Retrieve the exposed port for the given host port pair
   *
   * @param originalHostName the original host name
   * @param originalPort the original port
   * @return
   */
  public Integer getServicePort(String originalHostName, int originalPort) {
    return kafkaContainers.stream()
        .filter(kafkaContainer -> kafkaContainer.getNetworkAliases().contains(originalHostName))
        .filter(kafkaContainer -> kafkaContainer.getMappedPort(originalPort) != null)
        .findFirst()
        .map(kafkaContainer -> kafkaContainer.getMappedPort(originalPort))
        .orElseThrow(() -> new RuntimeException("Invalid Port"));
  }

  public KafkaKraftCluster withSchemaRegistry() {
    this.addSchemaRegistry = true;
    return this;
  }

  public KafkaKraftCluster withSchemaRegistry(
      final SchemaRegistryContainer schemaRegistryContainer) {
    this.addSchemaRegistry = true;
    this.schemaRegistryContainer = schemaRegistryContainer;
    return this;
  }

  public KafkaKraftCluster withSchemaRegistry(final String imageName, final String imageTag) {
    this.schemaRegistryContainer = new SchemaRegistryContainer(imageName, imageTag);
    this.addSchemaRegistry = true;
    return this;
  }

  public KafkaKraftCluster withSchemaRegistry(final DockerImageName dockerImageName) {
    this.schemaRegistryContainer = new SchemaRegistryContainer(dockerImageName);
    this.addSchemaRegistry = true;

    return this;
  }

  public KafkaKraftCluster withSchemaRegistry(final String imageTag) {
    this.schemaRegistryContainer = new SchemaRegistryContainer(imageTag);
    this.addSchemaRegistry = true;
    return this;
  }

  @Override
  public void start() {
    this.kafkaContainers = createKafkaContainers(numBrokers);

    if (kafkaContainers.isEmpty()) {
      throw new IllegalArgumentException("Must have at least one Kafka container");
    }

    // add additional container envs
    kafkaContainers.forEach(kafkaContainer -> kafkaContainer.withEnv(additionalContainerEnv));

    // start all the brokers at once
    kafkaContainers.stream().parallel().forEach(KafkaContainer::start);

    Unreliables.retryUntilTrue(
        600,
        TimeUnit.SECONDS,
        () -> {
          // We used to use the kafka-metadata-shell-sh tool
          // https://docs.confluent.io/kafka/operations-tools/kafka-tools.html#kafka-metadata-shell-sh
          // with ls /brokers but the behaviour changes from 7.4 to 7.5 so instead using the quorum
          // as a check.
          Container.ExecResult result =
              Objects.requireNonNull(kafkaContainers.stream().findFirst().orElse(null))
                  .execInContainer(
                      "sh",
                      "-c",
                      "kafka-metadata-quorum --bootstrap-server localhost:9092 describe --status");

          // output looks like this
          /*
           * ClusterId:              fMCL8kv1SWm87L_Md-I2hg
           * LeaderId:               3002
           * LeaderEpoch:            2
           * HighWatermark:          10
           * MaxFollowerLag:         0
           * MaxFollowerLagTimeMs:   -1
           * CurrentVoters:          [3000,3001,3002]
           * CurrentObservers:       [0,1,2]
           */
          // extract CurrentVoters and CurrentObservers
          String[] lines = result.getStdout().split("\n");
          String resultClusterId =
              Arrays.stream(lines)
                  .filter(line -> line.startsWith("ClusterId:"))
                  .findFirst()
                  .orElse("")
                  .split(":")[1]
                  .trim();
          String resultCurrentVoters =
              Arrays.stream(lines)
                  .filter(line -> line.startsWith("CurrentVoters:"))
                  .findFirst()
                  .orElse("")
                  .split(":")[1]
                  .trim();
          // convert currentVoters to a list of integers
          List<Integer> currentVoters =
              Arrays.stream(
                      resultCurrentVoters.substring(1, resultCurrentVoters.length() - 1).split(","))
                  .map(String::trim)
                  .map(Integer::parseInt)
                  .collect(Collectors.toList());

          return getClusterId().equals(resultClusterId) && currentVoters.size() == numBrokers;
        });

    // start schema registry after brokers. containers need to be started in order to get the mapped
    // ports for schema registry
    if (addSchemaRegistry) {
      runSchemaRegistryContainer();
    }
  }

  @Override
  public void stop() {
    if (isSchemaRegistryRunning()) {
      schemaRegistryContainer.stop();
    }
    kafkaContainers.parallelStream().forEach(KafkaContainer::stop);
  }

  /** @return is the schema registry running */
  public boolean isSchemaRegistryRunning() {
    if (schemaRegistryContainer == null) {
      return false;
    }
    return schemaRegistryContainer.isRunning();
  }

  private List<KafkaContainer> createKafkaContainers(int numBrokers) {
    if (numBrokers <= 0)
      throw new IllegalArgumentException("Must have at least one Kafka container");

    String controllerQuorumVoters = getControllerQuorumVoters(numBrokers);
    int internalTopicsReplicationFactor = getInternalTopicsReplicationFactor(numBrokers);

    if (internalTopicsReplicationFactor < 0 || internalTopicsReplicationFactor > numBrokers)
      throw new IllegalArgumentException(
          "Internal topics replication factor must be between 0 and the number of brokers");

    return IntStream.range(0, numBrokers)
        .mapToObj(
            brokerNum ->
                new KafkaContainer(dockerImageName)
                    .withNetwork(this.network)
                    .withNetworkAliases(String.format("broker-%s", brokerNum))
                    .withKraft()
                    .withClusterId(getClusterId())
                    .withEnv("KAFKA_BROKER_ID", String.valueOf(brokerNum))
                    .withEnv("KAFKA_NODE_ID", String.valueOf(brokerNum))
                    .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters)
                    .withEnv(
                        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR",
                        String.valueOf(internalTopicsReplicationFactor))
                    .withEnv(
                        "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS",
                        String.valueOf(internalTopicsReplicationFactor))
                    .withEnv(
                        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                        String.valueOf(internalTopicsReplicationFactor))
                    .withEnv(
                        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                        String.valueOf(internalTopicsReplicationFactor))
                    .withStartupTimeout(Duration.ofMinutes(waitTimeout)))
        .map(kafkaContainer -> reuseContainers ? kafkaContainer.withReuse(true) : kafkaContainer)
        .collect(Collectors.toList());
  }

  private void runSchemaRegistryContainer() {
    if (schemaRegistryContainer == null) {
      schemaRegistryContainer = new SchemaRegistryContainer(imageTag);
    }
    if (reuseContainers) {
      schemaRegistryContainer.withReuse(true);
    }
    schemaRegistryContainer.withNetwork(this.network);
    schemaRegistryContainer.withBootstrapServers(getBootstrapServersAlias());

    schemaRegistryContainer.start();
  }

  private String getControllerQuorumVoters(int numBrokers) {
    return IntStream.range(0, numBrokers)
        .mapToObj(brokerNum -> String.format("%d@broker-%d:9094", brokerNum, brokerNum))
        .collect(Collectors.joining(","));
  }

  private int getInternalTopicsReplicationFactor(int numBrokers) {
    return (int) Math.ceil(numBrokers / 2.0);
  }

  /**
   * Similar to {@link #getBootstrapServers()} but returns the network alias of the bootstrap
   * servers instead of the local mappings
   *
   * @return
   */
  public String getBootstrapServersAlias() {
    return kafkaContainers.stream()
        .map(
            kafkaContainer ->
                kafkaContainer.getNetworkAliases().stream()
                        .filter(alias -> alias.startsWith("broker-"))
                        .findFirst()
                        .get()
                    + ":9092")
        .collect(Collectors.joining(","));
  }

  private String generateClusterId() {
    return Uuid.randomUuid().toString();
  }

  /** @return the bootstrap servers for the Kafka cluster */
  public String getBootstrapServers() {
    return kafkaContainers.stream()
        .map(kafkaContainer -> kafkaContainer.getHost() + ":" + kafkaContainer.getMappedPort(9093))
        .collect(Collectors.joining(","));
  }
}
