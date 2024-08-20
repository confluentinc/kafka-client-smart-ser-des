/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.common.test.utils.containers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class KafkaCluster {

  @Getter protected ZookeeperContainer zkContainer;
  protected final List<KafkaContainer> akContainers = new ArrayList<>();

  @Getter private final Network network;

  public static final int DEFAULT_WAIT_TIMEOUT = 2;
  private int waitTimeout = DEFAULT_WAIT_TIMEOUT;

  public KafkaCluster() {
    this.network = Network.newNetwork();
  }

  public KafkaCluster(Network network) {
    this.network = network;
  }

  public static KafkaCluster defaultCluster() {
    final KafkaCluster cluster = KafkaCluster.newCluster();
    return cluster.withNodes(1);
  }

  public KafkaCluster withContainerEnv(String key, String value) {
    akContainers.forEach(kafkaContainer -> kafkaContainer.withEnv(key, value));
    return this;
  }

  /**
   * Set how long to wait (in minutes) before timing out when starting a cluster
   *
   * @param timeout the timeout period in minutes
   * @return this instance
   */
  public KafkaCluster withWaitTimeout(int timeout) {
    this.waitTimeout = timeout;
    return this;
  }

  public KafkaCluster withReuse(boolean reuse) {
    akContainers.forEach(kafkaContainer -> kafkaContainer.withReuse(reuse));
    zkContainer.withReuse(reuse);

    return this;
  }

  public boolean isRunning() {
    return zkContainer.isRunning()
        && akContainers.stream()
            .map(ContainerState::isRunning)
            .reduce(true, (previous, current) -> previous && current);
  }

  public KafkaContainer getContainer(int containerIndex) {
    return akContainers.get(containerIndex);
  }

  public static KafkaCluster newCluster() {
    return new KafkaCluster();
  }

  public static KafkaCluster newCluster(Network network) {
    return new KafkaCluster(network);
  }

  public int getNodeCount() {
    return akContainers.size();
  }

  public KafkaCluster withNodes(int nodeCount) {
    zkContainer = new ZookeeperContainer().withNetwork(network);

    for (int i = 0; i < nodeCount; i++) {
      KafkaContainer akContainer =
          new KafkaContainer(i)
              .withExternalZookeeperConnect(zkContainer.getAddress())
              .withNetwork(network)
              .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", Integer.toString(nodeCount))
              .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", Integer.toString(nodeCount))
              .waitingFor(
                  Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(waitTimeout)));
      akContainers.add(akContainer);
    }
    return this;
  }

  public KafkaCluster withNodes(
      final DockerImageName kafkaImageName,
      final DockerImageName zookeeperImageName,
      int nodeCount) {
    zkContainer = new ZookeeperContainer(zookeeperImageName).withNetwork(network);

    for (int i = 0; i < nodeCount; i++) {
      KafkaContainer akContainer =
          new KafkaContainer(kafkaImageName, Integer.toString(i))
              .withExternalZookeeperConnect(zkContainer.getAddress())
              .withNetwork(network)
              .waitingFor(
                  Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(waitTimeout)));
      akContainers.add(akContainer);
    }
    return this;
  }

  public KafkaCluster buildNodes(int nodeCount, KafkaContainerBuilder builder) {
    for (int i = 0; i < nodeCount; i++) {
      akContainers.add(
          builder
              .build(i)
              .withNetwork(network)
              .withExternalZookeeperConnect(zkContainer.getAddress())
              .waitingFor(
                  Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(waitTimeout))));
    }
    return this;
  }

  public KafkaCluster withKafkaContainers(Collection<KafkaContainer> containers) {
    for (KafkaContainer container : containers) {
      akContainers.add(
          container
              .withNetwork(network)
              .withExternalZookeeperConnect(zkContainer.getAddress())
              .waitingFor(
                  Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(waitTimeout))));
    }
    return this;
  }

  public KafkaCluster withZookeeper() {
    return withZkContainer(new ZookeeperContainer());
  }

  public KafkaCluster withZookeeper(final DockerImageName zookeeperImageName) {
    return withZkContainer(new ZookeeperContainer(zookeeperImageName));
  }

  public KafkaCluster withZkContainer(ZookeeperContainer container) {
    zkContainer = container.withNetwork(network);
    return this;
  }

  /** Starts the cluster */
  public void start() {
    zkContainer.start();
    for (KafkaContainer akContainer : akContainers) {
      akContainer.start();
    }
  }

  public String getServiceHost(String originalHostname, int originalPort) {
    return akContainers.stream()
        .filter(c -> c.getHostAlias().equals(originalHostname) && c.getKafkaPort() == originalPort)
        .findFirst()
        .map(ContainerState::getHost)
        .orElseThrow(() -> new RuntimeException("Invalid Host"));
  }

  public Integer getServicePort(String originalHostname, int originalPort) {
    return akContainers.stream()
        .filter(c -> c.getHostAlias().equals(originalHostname) && c.getKafkaPort() == originalPort)
        .findFirst()
        .map(KafkaContainer::getPort)
        .orElseThrow(() -> new RuntimeException("Invalid Port"));
  }

  /** Stops the cluster */
  public void stop() {
    for (KafkaContainer akContainer : akContainers) {
      akContainer.stop();
    }
    zkContainer.stop();
  }

  public String getBootstrapServers() {
    return akContainers.stream()
        .map(KafkaContainer::getBootstrapServers)
        .collect(Collectors.joining(","));
  }

  public String getContainerLogs() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < akContainers.size(); i++) {
      sb.append("--- CONTAINER " + i + " LOGS ---")
          .append("\n" + akContainers.get(i).getLogs() + "\n\n");
    }
    return sb.toString();
  }

  public interface KafkaContainerBuilder {
    KafkaContainer build(int index);
  }
}
