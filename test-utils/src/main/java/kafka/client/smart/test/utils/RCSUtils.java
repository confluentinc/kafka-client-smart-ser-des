/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import com.google.common.base.Charsets;
import com.google.common.io.ByteSource;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;

@UtilityClass
@SuppressWarnings({"unused"})
public class RCSUtils {

  public InputStream getResourceAsStream(final String name, final Class<?> rcsClass) {
    return rcsClass.getResourceAsStream(name);
  }

  public String getResourceAsString(final String name, final Class<?> rcsClass) throws IOException {
    final ByteSource byteSource =
        new ByteSource() {
          @Override
          public InputStream openStream() {
            return getResourceAsStream(name, rcsClass);
          }
        };

    return byteSource.asCharSource(Charsets.UTF_8).read();
  }

  @SneakyThrows
  public Path getResourcePath(final String rcs, final Class<?> tClass) {
    String path = Objects.requireNonNull(tClass.getResource(rcs)).getFile();
    if (path.contains(".jar!")) {
      InputStream stream = getResourceAsStream(rcs, tClass);
      assert stream != null;

      Path tempDir = Paths.get(Files.createTempDirectory("test-utils").toFile().getAbsolutePath());
      File file = tempDir.resolve(UUID.randomUUID().toString()).toFile();
      Files.copy(stream, file.toPath());

      path = file.getAbsolutePath();
    }
    return Paths.get(path);
  }

  public Map<String, Object> getResourceAsMap(final String name, final Class<?> rcsClass)
      throws IOException {
    return ExtendedProperties.loadFromResource(name, rcsClass).toMap();
  }
}
