/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.common.schema;

import csid.client.common.exception.ConfluentSchemaException;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility class for Schema Registry
 */
public final class SchemaRegistryUtils {

    private static final String MOCK_URL_PREFIX = "mock://";

    public final static int NO_SCHEMA_ID = -1;

    /**
     * Get Schema ID
     *
     * @param bytes Byte array
     * @return Schema ID
     */
    public static int getSchemaId(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int magicByte = buffer.get();
        if (magicByte != 0) {
            return NO_SCHEMA_ID;
        }

        return buffer.getInt();
    }

    public static boolean isSchemaRegistryConfigured(Map<String, ?> configs) {
        return configs.containsKey(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
    }

    /**
     * Get Schema Type
     *
     * @param supplier Schema Registry Client Supplier
     * @param bytes    Byte array
     * @return Schema Type
     * @throws RestClientException Rest Client Exception
     * @throws IOException         IO Exception
     */
    public static String getSchemaType(Supplier<SchemaRegistryClient> supplier, byte[] bytes) throws RestClientException, IOException {
        final int schemaId = getSchemaId(bytes);
        if (schemaId == NO_SCHEMA_ID) {
            return null;
        }

        final SchemaRegistryClient schemaRegistryClient = supplier.get();
        if (schemaRegistryClient == null) {
            return null;
        }

        final ParsedSchema parsedSchema = supplier.get().getSchemaById(schemaId);
        return (parsedSchema == null)
                ? null
                : parsedSchema.schemaType();
    }

    /**
     * Get Schema Registry Client
     *
     * @param props Map of properties
     * @return Schema Registry Client
     */
    public static SchemaRegistryClient getSchemaRegistryClient(Map<?, ?> props) {
        SchemaRegistryClientConfig config = new SchemaRegistryClientConfig(props);

        List<String> urls = config.getSchemaRegistryUrls();

        // Validate urls
        urls.forEach(url -> {
            if (!url.startsWith(MOCK_URL_PREFIX)) {
                try {
                    new URL(url);
                } catch (MalformedURLException exception) {
                    throw new ConfluentSchemaException("Malformed URL: " + url, exception);
                }
            }
        });

        // Determine if this is a mock schema registry
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");
        String mockScope = validateAndMaybeGetMockScope(urls);
        List<SchemaProvider> providers = Arrays.asList(new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider());

        SchemaRegistryClient schemaRegistry;
        if (mockScope != null) {
            schemaRegistry = MockSchemaRegistry.getClientForScope(mockScope, providers);
        } else {
            schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject, providers, originals, config.requestHeaders());
        }

        return schemaRegistry;
    }

    /**
     * Validate and get mock scope
     *
     * @param urls List of URLs
     * @return Mock scope
     */
    private static String validateAndMaybeGetMockScope(List<String> urls) {
        List<String> mockScopes = urls.stream()
                .filter(url -> url.startsWith(MOCK_URL_PREFIX))
                .map(url -> url.substring(MOCK_URL_PREFIX.length()))
                .collect(Collectors.toList());

        if (mockScopes.isEmpty()) {
            return null;
        } else if (mockScopes.size() > 1) {
            throw new ConfigException("Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls);
        } else if (urls.size() > mockScopes.size()) {
            throw new ConfigException("Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls);
        }

        return mockScopes.get(0);
    }

    private SchemaRegistryUtils() {
    }
}

