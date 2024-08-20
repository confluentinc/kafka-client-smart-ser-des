/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.io.IOException;
import java.io.InputStream;

import lombok.experimental.UtilityClass;
import org.apache.avro.Schema;

@UtilityClass
@SuppressWarnings({"unused"})
public class SRUtils {
  public final String SCOPE = "test";

  public void reset() {
    MockSchemaRegistry.dropScope(SCOPE);
  }

  public SchemaRegistryClient getSRClient() {
    return MockSchemaRegistry.getClientForScope("test");
  }

  public static void registerSchema(
      final String subject, final String schemaResourceName, final Class<?> rcsClass)
      throws IOException, RestClientException {
    String schemaSource =
        isJson(schemaResourceName)
            ? RCSUtils.getResourceAsString(schemaResourceName, rcsClass)
            : AvroUtils.getAvroSource(schemaResourceName);
    registerSchema(subject, schemaSource);
  }

  private static boolean isJson(String schemaResourceName) {
    return schemaResourceName.contains(".json");
  }

  public String getSRClientURL() {
    return "mock://test";
  }

  public void registerSchema(final String subject, final InputStream inputStream)
      throws IOException, RestClientException {
    final Schema catalogSchema = new Schema.Parser().parse(inputStream);
    MockSchemaRegistry.getClientForScope(SCOPE).register(subject, new AvroSchema(catalogSchema));
  }

  public void registerSchema(final String subject, final Schema schema)
      throws IOException, RestClientException {
    MockSchemaRegistry.getClientForScope(SCOPE).register(subject, new AvroSchema(schema));
  }

  public void registerSchema(final String subject, final String schema)
      throws IOException, RestClientException {
    MockSchemaRegistry.getClientForScope(SCOPE)
        .register(subject, new AvroSchema(new Schema.Parser().parse(schema)));
  }
}
