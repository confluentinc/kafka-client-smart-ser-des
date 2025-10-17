/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils.containers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import kafka.client.smart.test.utils.Environment;
import kafka.client.smart.test.utils.containers.ContainerUtils;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.shaded.com.google.common.collect.Lists;
import org.testcontainers.utility.DockerImageName;

/** A Kafka Docker container */
@Slf4j
public class KafkaContainer extends GenericContainer<KafkaContainer> {

  public static final DockerImageName DEFAULT_IMAGE;

  static {
    DEFAULT_IMAGE =
        (Environment.isARM())
            ? DockerImageName.parse("confluentinc/cp-kafka").withTag(ContainerUtils.DEFAULT_CP_IMAGE_TAG + ".arm64")
            : DockerImageName.parse("confluentinc/cp-kafka").withTag(ContainerUtils.DEFAULT_CP_IMAGE_TAG);
  }

  private static final String STARTER_SCRIPT = "/testcontainers_start.sh";
  public static final String EXTERNAL_LISTENER_NAME = "PLAINTEXT";
  public static final String INTERNAL_LISTENER_NAME = "BROKER";
  public static final String DEFAULT_LISTENER_PROTOCOL = "PLAINTEXT";
  public static final int KAFKA_PORT = 9092;
  public static final int KAFKA_INTERNAL_PORT = 9093;
  public static final int ZOOKEEPER_PORT = 2181;

  private static final int PORT_NOT_ASSIGNED = -1;
  private int port = PORT_NOT_ASSIGNED;
  protected String externalZookeeperConnect;
  private final String brokerId;
  private final String hostAlias;

  private String internalListener;
  private String internalListenerMap;
  String externalListener = EXTERNAL_LISTENER_NAME + "://0.0.0.0:" + KAFKA_PORT;
  String externalListenerMap = EXTERNAL_LISTENER_NAME + ":" + DEFAULT_LISTENER_PROTOCOL;

  public int getPort() {
    return port;
  }

  public String getHostAlias() {
    return hostAlias;
  }

  public KafkaContainer() {
    this(DEFAULT_IMAGE, null);
  }

  public KafkaContainer(Integer brokerId) {
    this(DEFAULT_IMAGE, brokerId.toString());
  }

  public KafkaContainer(final DockerImageName dockerImageName, String brokerId) {
    super(dockerImageName);

    log.info("VM using '{}'.", dockerImageName.asCanonicalNameString());

    this.brokerId = brokerId;
    this.hostAlias = "broker" + brokerId;
    withNetworkAliases(hostAlias);
    withExposedPorts(getKafkaPort());
    if (brokerId != null && !brokerId.isEmpty()) {
      withEnv("KAFKA_BROKER_ID", brokerId);
    }
    withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
    withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1");
    withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "");
    withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    internalListener = INTERNAL_LISTENER_NAME + "://" + hostAlias + ":" + getKafkaInternalPort();
    internalListenerMap = INTERNAL_LISTENER_NAME + ":" + DEFAULT_LISTENER_PROTOCOL;
    updateListeners();
  }

  /* Note difference between 0.0.0.0 and localhost: The former will be replaced by the container IP. */
  protected String getProtocolMap() {
    return Lists.newArrayList(externalListenerMap, internalListenerMap).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.joining(","));
  }

  protected String getListeners() {
    return Lists.newArrayList(externalListener, internalListener).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.joining(","));
  }

  public KafkaContainer withInternalListener(String listener, String protocol) {
    internalListener = listener + "://" + hostAlias + ":" + getKafkaInternalPort();
    internalListenerMap = listener + ":" + protocol;
    updateListeners();
    return this;
  }

  public KafkaContainer withExternalListener(String listener, String protocol) {
    externalListener = listener + "://0.0.0.0:" + KAFKA_PORT;
    externalListenerMap = listener + ":" + protocol;
    updateListeners();
    return this;
  }

  private void updateListeners() {
    withEnv("KAFKA_LISTENERS", getListeners());
    withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", getProtocolMap());
    withEnv(
        "KAFKA_INTER_BROKER_LISTENER_NAME",
        (internalListener != null)
            ? internalListener.split(":")[0]
            : externalListener.split(":")[0]);
  }

  public KafkaContainer withExternalZookeeperConnect(String zookeeperConnect) {
    this.externalZookeeperConnect = zookeeperConnect;
    return this;
  }

  /**
   * Enables debug level logging on the kafka cluster
   *
   * @return a KafkaContainer with debug logging enabled
   */
  public KafkaContainer withDebugLogging() {
    withEnv(
        "KAFKA_LOG4J_LOGGERS",
        "kafka.server=DEBUG,kafka.controller=DEBUG,kafka.request.logger=DEBUG");
    withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", "DEBUG");
    return this;
  }

  public int getKafkaPort() {
    return KAFKA_PORT;
  }

  public int getKafkaInternalPort() {
    return KAFKA_INTERNAL_PORT + Integer.parseInt(brokerId);
  }

  @Override
  protected final void doStart() {
    withCommand(
        "sh", "-c", "while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);
    if (externalZookeeperConnect == null) {
      addExposedPort(ZOOKEEPER_PORT);
    }
    beforeStart();
    super.doStart();
  }

  /** Subclasses may override. */
  protected void beforeStart() {
    // subclasses may override
  }

  @Override
  protected final void containerIsStarting(
      final InspectContainerResponse containerInfo, final boolean reused) {
    super.containerIsStarting(containerInfo, reused);
    port = getMappedPort(getKafkaPort());
    if (reused) {
      return;
    }
    beforeStartupPreparations();
    createStartupScript(externalZookeeperConnect);
  }

  /** Subclasses may override. */
  protected void beforeStartupPreparations() {
    // Subclasses may override
  }

  private void createStartupScript(final String zookeeperConnect) {
    final String listeners = getEnvMap().get("KAFKA_LISTENERS");
    if (listeners == null) {
      throw new RuntimeException("Need environment variable KAFKA_LISTENERS");
    }
    final String advertisedListeners =
        overrideAdvertisedListeners(
            listeners
                .replaceAll(":" + getKafkaPort(), ":" + getMappedPort(getKafkaPort()))
                .replaceAll("OTHER://0\\.0\\.0\\.0", "OTHER://kafka")
                .replaceAll("0\\.0\\.0\\.0", getHost()));

    final String startupScript =
        overrideStartupScript(
            "#!/bin/bash\n"
                + "export KAFKA_ZOOKEEPER_CONNECT='"
                + zookeeperConnect
                + "'\n"
                + "export KAFKA_ADVERTISED_LISTENERS='"
                + advertisedListeners
                + "'\n"
                + ". /etc/confluent/docker/bash-config\n"
                + "/etc/confluent/docker/configure\n"
                + "env\n"
                + "/etc/confluent/docker/launch\n");
    copyFileToContainer(
        Transferable.of(startupScript.getBytes(StandardCharsets.UTF_8), 0755), STARTER_SCRIPT);
  }

  /** Subclasses may override. */
  protected String overrideAdvertisedListeners(final String advertisedListeners) {
    return advertisedListeners;
  }

  /** Subclasses may override. */
  protected String overrideStartupScript(final String startupScript) {
    return startupScript;
  }

  /** Enables remote debugging on port 5005 */
  public void enableRemoteDebug() {
    enableRemoteDebug(5005);
  }

  public KafkaContainer withRemoteDebug() {
    enableRemoteDebug();
    return this;
  }

  public KafkaContainer withRemoteDebug(int port) {
    enableRemoteDebug(port);
    return this;
  }
  /**
   * Enables remote debugging on the given port
   *
   * @param port the port to listen for debug sessions on
   */
  public void enableRemoteDebug(int port) {
    withEnv(
        "EXTRA_ARGS",
        "-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=*:" + port + ",suspend=n");
    addFixedExposedPort(port, port);
  }

  public String getBootstrapServers() {
    return String.format("%s:%s", getHost(), getPort());
  }
}
