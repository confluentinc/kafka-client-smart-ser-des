/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package io.confluent.common.test.utils.containers;

import io.confluent.common.test.utils.Environment;
import lombok.experimental.UtilityClass;
import org.testcontainers.utility.DockerImageName;

/** Utility class for containers. */
@UtilityClass
public class ContainerUtils {

  public final String DEFAULT_CP_IMAGE_TAG = "7.5.1";

  /**
   * Determines the Docker image name to use based on the current environment. Confluent appends the
   * image tag with "arm64" for ARM-based environments.
   *
   * @param imageName the image name of the kafka cluster to use
   * @param imageTag the image tag of the kafka cluster to use
   * @return the Docker image name to use
   */
  public DockerImageName getDockerImageNameFrom(String imageName, String imageTag) {
    return (Environment.isARM())
        ? DockerImageName.parse(imageName).withTag(imageTag.concat("arm64"))
        : DockerImageName.parse(imageName).withTag(imageTag);
  }
}
