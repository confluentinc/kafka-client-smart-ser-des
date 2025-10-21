/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import kafka.client.smart.common.ConfigurationUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
@SuppressWarnings({"unused"})
public class RCSUtils {

  public InputStream getResourceAsStream(final String name, final Class<?> rcsClass) {
    return rcsClass.getResourceAsStream(name);
  }

  public String getResourceAsString(final String name, final Class<?> rcsClass) throws IOException {
    return ConfigurationUtils.getResourceAsString(name, rcsClass);
  }

  public Path getResourcePath(final String rcs, final Class<?> tClass) throws IOException {
    return ConfigurationUtils.getResourcePath(rcs, tClass);
  }

  public Map<String, Object> getResourceAsMap(final String name, final Class<?> rcsClass)
      throws IOException {
    return ExtendedProperties.loadFromResource(name, rcsClass).toMap();
  }
}
