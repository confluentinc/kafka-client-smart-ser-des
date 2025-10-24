/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils.containers;

import kafka.client.smart.test.utils.Environment;
import org.testcontainers.utility.DockerImageName;

/** Utility class for containers. */
public final class ContainerUtils {

  public static final String DEFAULT_CP_IMAGE_TAG = "7.7.1"; 

  private ContainerUtils() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  /**
   * Determines the Docker image name to use based on the current environment.
   * Confluent appends the image tag with "arm64" for ARM-based environments.
   */
  public static DockerImageName getDockerImageNameFrom(String imageName, String imageTag) {
    return Environment.isARM()
        ? DockerImageName.parse(imageName).withTag(imageTag.concat("arm64"))
        : DockerImageName.parse(imageName).withTag(imageTag);
  }
}
