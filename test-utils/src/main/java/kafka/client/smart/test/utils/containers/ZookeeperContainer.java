/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils.containers;

import kafka.client.smart.test.utils.Environment;
import kafka.client.smart.test.utils.containers.ContainerUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

  public static final DockerImageName DEFAULT_IMAGE;

  static {
    DEFAULT_IMAGE =
        (Environment.isARM())
            ? DockerImageName.parse("confluentinc/cp-zookeeper").withTag(ContainerUtils.DEFAULT_CP_IMAGE_TAG + ".arm64")
            : DockerImageName.parse("confluentinc/cp-zookeeper").withTag(ContainerUtils.DEFAULT_CP_IMAGE_TAG);
  }

  public ZookeeperContainer() {
    this(DEFAULT_IMAGE);
  }

  public ZookeeperContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    withEnv(zkEnvConfig()).withNetworkAliases("zookeeper");

    log.info("VM using '{}'.", dockerImageName.asCanonicalNameString());
  }

  public String getAddress() {
    return "zookeeper:2181";
  }

  private static Map<String, String> zkEnvConfig() {
    Map<String, String> config = new HashMap<>();
    config.put("ZOOKEEPER_CLIENT_PORT", "2181");
    config.put("ZOOKEEPER_TICK_TIME", "2000");
    return config;
  }
}
