/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import kafka.client.smart.common.ConfigurationUtils;
import kafka.client.smart.test.utils.containers.KafkaCluster;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

/**
 * Base class for integration tests that provides common setup and teardown functionality.
 * This eliminates code duplication across test classes by centralizing:
 * - File cleanup operations
 * - Kafka cluster management
 * - Schema Registry reset
 * - Configuration file creation
 * - Connect setup/teardown
 */
@Slf4j
public abstract class BaseIntegrationTest {

    protected KafkaCluster cluster;

    // Common test files that need cleanup
    private static final String[] COMMON_TEST_FILES = {
        "/tmp/connect-standalone.properties",
        "/tmp/connector.properties", 
        "/tmp/connector_sink.properties",
        "/tmp/test.sink.txt"
    };

    @BeforeEach
    public void setup() throws IOException {
        log.debug("Setting up test environment");
        
        // Clean up any existing test files
        cleanupTestFiles();
        
        // Start Kafka cluster
        cluster = KafkaCluster.defaultCluster().withReuse(true);
        cluster.start();
        
        // Reset Schema Registry
        SRUtils.reset();
        
        // Create configuration files
        createConfigFile();
        createConnectorConfig("connector_sink.properties", "connector_sink.properties");
        
        // Create test sink file
        new File("/tmp/test.sink.txt").createNewFile();
        
        log.debug("Test environment setup complete");
    }

    @AfterEach
    public void teardown() {
        log.debug("Tearing down test environment");
        
        // Stop Kafka cluster
        if (cluster != null) {
            cluster.stop();
        }
        
        // Clean up test files
        cleanupTestFiles();
        
        log.debug("Test environment teardown complete");
    }

    /**
     * Clean up all common test files
     */
    protected void cleanupTestFiles() {
        for (String filename : COMMON_TEST_FILES) {
            deleteFile(filename);
        }
    }

    /**
     * Delete a specific file if it exists
     * 
     * @param filename The file to delete
     */
    protected void deleteFile(String filename) {
        File file = new File(filename);
        if (file.exists()) {
            boolean deleted = file.delete();
            if (!deleted) {
                log.warn("Failed to delete file: {}", filename);
            }
        }
    }

    /**
     * Create the connect standalone configuration file
     * 
     * @throws IOException if file creation fails
     */
    protected void createConfigFile() throws IOException {
        String config = ConfigurationUtils.getResourceAsString("/connect-standalone.properties", getClass());
        config = ConfigurationUtils.replacePlaceholder(config, "{{bootstrap.servers}}", cluster.getBootstrapServers());

        File configFile = new File("/tmp/connect-standalone.properties");
        if (configFile.exists()) {
            configFile.delete();
        }

        try (FileWriter writer = new FileWriter("/tmp/connect-standalone.properties")) {
            writer.write(config);
        }
    }

    /**
     * Create connector configuration file
     * 
     * @param srcFilename Source filename in resources
     * @param targetFilename Target filename to create
     * @throws IOException if file creation fails
     */
    protected void createConnectorConfig(String srcFilename, String targetFilename) throws IOException {
        final Path schema = ConfigurationUtils.getResourcePath("/emp.avsc", getClass());
        String config = ConfigurationUtils.getResourceAsString("/" + srcFilename, getClass());
        
        Map<String, String> replacements = Map.of(
            "{{bootstrap.servers}}", cluster.getBootstrapServers(),
            "{{schema.registry.url}}", SRUtils.getSRClientURL(),
            "{{schema}}", schema.toString()
        );
        config = ConfigurationUtils.replacePlaceholders(config, replacements);

        try (FileWriter writer = new FileWriter("/tmp/" + targetFilename)) {
            writer.write(config);
        }
    }

    /**
     * Get the Kafka cluster instance
     * 
     * @return The Kafka cluster
     */
    protected KafkaCluster getCluster() {
        return cluster;
    }
}
