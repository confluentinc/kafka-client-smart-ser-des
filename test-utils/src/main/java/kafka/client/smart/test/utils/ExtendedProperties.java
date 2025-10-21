/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import kafka.client.smart.common.ConfigurationUtils;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class ExtendedProperties extends Properties {

  public static ExtendedProperties loadFromResource(final String name, final Class<?> rcsClass)
      throws IOException {
    final ExtendedProperties properties = new ExtendedProperties();
    properties.putAll(ConfigurationUtils.loadFromResource(name, rcsClass));
    return properties;
  }

  /** Load {@link Properties} from a give file */
  public static ExtendedProperties loadProperties(String filePath) throws IOException {
    ExtendedProperties properties = new ExtendedProperties();
    properties.putAll(ConfigurationUtils.loadFromFile(filePath));
    return properties;
  }

  /** Load {@link Properties} from a give file. Fall back to resource if file not found */
  public Properties loadProperties(String filePath, Class<?> rcsClass) throws IOException {
    return ConfigurationUtils.loadFromFileOrResource(filePath, filePath, rcsClass);
  }

  public ExtendedProperties replace(final String key, final String value) {
    putAll(ConfigurationUtils.replaceInValues(this, key, value));
    return this;
  }

  public ExtendedProperties withProperty(final String key, final String value) {
    ConfigurationUtils.withProperty(this, key, value);
    return this;
  }

  public Map<String, Object> toMap() {
    return ConfigurationUtils.propertiesToMap(this);
  }
}
