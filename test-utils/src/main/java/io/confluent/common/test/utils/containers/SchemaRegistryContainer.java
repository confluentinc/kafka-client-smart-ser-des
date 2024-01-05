/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package io.confluent.common.test.utils.containers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {

  private static final int SCHEMA_REGISTRY_PORT = 8081;

  private static final String DEFAULT_SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry";
  private static final DockerImageName DEFAULT_IMAGE =
      ContainerUtils.getDockerImageNameFrom(
          DEFAULT_SCHEMA_REGISTRY_IMAGE, ContainerUtils.DEFAULT_CP_IMAGE_TAG);
  private String imageName = DEFAULT_SCHEMA_REGISTRY_IMAGE;
  private String imageTag = ContainerUtils.DEFAULT_CP_IMAGE_TAG;
  private DockerImageName dockerImageName = DEFAULT_IMAGE;

  public SchemaRegistryContainer() {
    super(DEFAULT_IMAGE);
  }

  public SchemaRegistryContainer(final String imageName, final String imageTag) {
    this(ContainerUtils.getDockerImageNameFrom(imageName, imageTag));
  }

  public SchemaRegistryContainer(final DockerImageName dockerImageName) {
    super(dockerImageName);
    this.dockerImageName = dockerImageName;
    this.imageName = dockerImageName.getUnversionedPart();
    this.imageTag = dockerImageName.getVersionPart();
    withExposedPorts(SCHEMA_REGISTRY_PORT);
    withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry");
    withEnv("SCHEMA_REGISTRY_LISTENERS", String.format("http://0.0.0.0:%d", SCHEMA_REGISTRY_PORT));
    withNetworkAliases("schema-registry");
    waitingFor(Wait.forHttp("/subjects").forStatusCode(200));
  }

  public SchemaRegistryContainer(final String imageTag) {
    this(ContainerUtils.getDockerImageNameFrom(DEFAULT_SCHEMA_REGISTRY_IMAGE, imageTag));
  }

  public SchemaRegistryContainer withBootstrapServers(final String bootstrapServers) {
    withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", bootstrapServers);
    return self();
  }

  public SchemaRegistryContainer withContainerEnv(final String key, final String value) {
    withEnv(key, value);
    return self();
  }

  /** @return the Schema Registry URL */
  public String getSchemaRegistryUrl() {
    return String.format("http://%s:%s", this.getHost(), this.getSchemaRegistryPort());
  }

  /** @return the Schema Registry port */
  public int getSchemaRegistryPort() {
    return this.getMappedPort(SCHEMA_REGISTRY_PORT);
  }
}
