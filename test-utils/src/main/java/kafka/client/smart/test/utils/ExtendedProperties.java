/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ExtendedProperties extends Properties {

  public static ExtendedProperties loadFromResource(final String name, final Class<?> rcsClass)
      throws IOException {
    final ExtendedProperties properties = new ExtendedProperties();
    properties.load(rcsClass.getResourceAsStream(name));
    return properties;
  }

  /** Load {@link Properties} from a give file */
  public static ExtendedProperties loadProperties(String filePath) throws IOException {
    ExtendedProperties properties = new ExtendedProperties();
    FileInputStream input = new FileInputStream(filePath);
    properties.load(input);
    input.close();
    return properties;
  }

  /** Load {@link Properties} from a give file. Fall back to resource if file not found */
  public Properties loadProperties(String filePath, Class<?> rcsClass) throws IOException {
    Properties properties = new Properties();

    try {
      FileInputStream input = new FileInputStream(filePath);
      properties.load(input);
      input.close();

    } catch (IOException e) {
      InputStream inputStream = rcsClass.getClassLoader().getResourceAsStream(filePath);
      if (inputStream == null) {
        throw e;
      }

      properties.load(inputStream);
    }

    return properties;
  }

  public ExtendedProperties replace(final String key, final String value) {
    forEach(
        (key1, value1) -> {
          if (value1.toString().contains(key)) {
            put(key1, value1.toString().replace(key, value));
          }
        });
    return this;
  }

  public ExtendedProperties withProperty(final String key, final String value) {
    put(key, value);
    return this;
  }

  public Map<String, Object> toMap() {
    final Map<String, Object> props = new HashMap<>();
    forEach((k, v) -> props.put(k.toString(), v));
    return props;
  }
}
