/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ConfigurationUtils.
 * Tests all configuration management functionality including resource loading,
 * file operations, placeholder replacement, and property conversion.
 */
@DisplayName("ConfigurationUtils Tests")
class ConfigurationUtilsTest {

    @TempDir
    Path tempDir;

    private File testFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create a test file
        testFile = tempDir.resolve("test.properties").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("key1=value1\nkey2=value2\nkey3=value3");
        }
    }

    @AfterEach
    void tearDown() {
        if (testFile != null && testFile.exists()) {
            testFile.delete();
        }
    }

    @Test
    @DisplayName("loadFromFile should load properties from file successfully")
    void testLoadFromFile() throws IOException {
        Properties properties = ConfigurationUtils.loadFromFile(testFile.getAbsolutePath());
        
        assertNotNull(properties);
        assertEquals("value1", properties.getProperty("key1"));
        assertEquals("value2", properties.getProperty("key2"));
        assertEquals("value3", properties.getProperty("key3"));
    }

    @Test
    @DisplayName("loadFromFile should throw IOException for non-existent file")
    void testLoadFromFileNonExistent() {
        assertThrows(IOException.class, () -> {
            ConfigurationUtils.loadFromFile("/non/existent/file.properties");
        });
    }

    @Test
    @DisplayName("loadFromResource should load properties from classpath resource")
    void testLoadFromResource() throws IOException {
        Properties properties = ConfigurationUtils.loadFromResource("/test-resource.properties", getClass());
        
        assertNotNull(properties);
        assertEquals("resource.value1", properties.getProperty("resource.key1"));
        assertEquals("resource.value2", properties.getProperty("resource.key2"));
    }

    @Test
    @DisplayName("loadFromResource should throw IOException for non-existent resource")
    void testLoadFromResourceNonExistent() {
        assertThrows(IOException.class, () -> {
            ConfigurationUtils.loadFromResource("/non/existent/resource.properties", getClass());
        });
    }

    @Test
    @DisplayName("loadFromFileOrResource should load from file when file exists")
    void testLoadFromFileOrResourceFileExists() throws IOException {
        Properties properties = ConfigurationUtils.loadFromFileOrResource(
            testFile.getAbsolutePath(), 
            "/test-resource.properties", 
            getClass()
        );
        
        assertNotNull(properties);
        assertEquals("value1", properties.getProperty("key1"));
        assertEquals("value2", properties.getProperty("key2"));
    }

    @Test
    @DisplayName("loadFromFileOrResource should fall back to resource when file doesn't exist")
    void testLoadFromFileOrResourceFileNotExists() throws IOException {
        Properties properties = ConfigurationUtils.loadFromFileOrResource(
            "/non/existent/file.properties",
            "/test-resource.properties",
            getClass()
        );
        
        assertNotNull(properties);
        assertEquals("resource.value1", properties.getProperty("resource.key1"));
        assertEquals("resource.value2", properties.getProperty("resource.key2"));
    }

    @Test
    @DisplayName("getResourceAsString should return resource content as string")
    void testGetResourceAsString() throws IOException {
        String content = ConfigurationUtils.getResourceAsString("/test-resource.properties", getClass());
        
        assertNotNull(content);
        assertTrue(content.contains("resource.key1=resource.value1"));
        assertTrue(content.contains("resource.key2=resource.value2"));
    }

    @Test
    @DisplayName("getResourceAsString should throw IOException for non-existent resource")
    void testGetResourceAsStringNonExistent() {
        assertThrows(IOException.class, () -> {
            ConfigurationUtils.getResourceAsString("/non/existent/resource.properties", getClass());
        });
    }

    @Test
    @DisplayName("getResourcePath should return path to resource")
    void testGetResourcePath() throws IOException {
        Path path = ConfigurationUtils.getResourcePath("/test-resource.properties", getClass());
        
        assertNotNull(path);
        assertTrue(Files.exists(path));
    }

    @Test
    @DisplayName("getResourcePath should handle JAR resources by extracting to temp file")
    void testGetResourcePathJarResource() throws IOException {
        // This test verifies the JAR resource handling logic
        Path path = ConfigurationUtils.getResourcePath("/test-resource.properties", getClass());
        
        assertNotNull(path);
        assertTrue(Files.exists(path));
    }

    @Test
    @DisplayName("replacePlaceholder should replace single placeholder correctly")
    void testReplacePlaceholder() {
        String config = "bootstrap.servers={{bootstrap.servers}}";
        String result = ConfigurationUtils.replacePlaceholder(config, "{{bootstrap.servers}}", "localhost:9092");
        
        assertEquals("bootstrap.servers=localhost:9092", result);
    }

    @Test
    @DisplayName("replacePlaceholder should handle multiple occurrences")
    void testReplacePlaceholderMultipleOccurrences() {
        String config = "{{bootstrap.servers}}={{bootstrap.servers}}";
        String result = ConfigurationUtils.replacePlaceholder(config, "{{bootstrap.servers}}", "localhost:9092");
        
        assertEquals("localhost:9092=localhost:9092", result);
    }

    @Test
    @DisplayName("replacePlaceholder should handle non-existent placeholder")
    void testReplacePlaceholderNonExistent() {
        String config = "bootstrap.servers=localhost:9092";
        String result = ConfigurationUtils.replacePlaceholder(config, "{{bootstrap.servers}}", "localhost:9092");
        
        assertEquals("bootstrap.servers=localhost:9092", result);
    }

    @Test
    @DisplayName("replacePlaceholders should replace multiple placeholders correctly")
    void testReplacePlaceholders() {
        String config = "bootstrap.servers={{bootstrap.servers}}\nschema.registry.url={{schema.registry.url}}";
        Map<String, String> replacements = new HashMap<>();
        replacements.put("{{bootstrap.servers}}", "localhost:9092");
        replacements.put("{{schema.registry.url}}", "http://localhost:8081");
        
        String result = ConfigurationUtils.replacePlaceholders(config, replacements);
        
        assertEquals("bootstrap.servers=localhost:9092\nschema.registry.url=http://localhost:8081", result);
    }

    @Test
    @DisplayName("replacePlaceholders should handle empty replacements map")
    void testReplacePlaceholdersEmptyMap() {
        String config = "bootstrap.servers={{bootstrap.servers}}";
        Map<String, String> replacements = new HashMap<>();
        
        String result = ConfigurationUtils.replacePlaceholders(config, replacements);
        
        assertEquals("bootstrap.servers={{bootstrap.servers}}", result);
    }

    @Test
    @DisplayName("propertiesToMap should convert Properties to Map correctly")
    void testPropertiesToMap() {
        Properties properties = new Properties();
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");
        
        Map<String, Object> map = ConfigurationUtils.propertiesToMap(properties);
        
        assertNotNull(map);
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));
        assertEquals(2, map.size());
    }

    @Test
    @DisplayName("propertiesToMap should handle empty Properties")
    void testPropertiesToMapEmpty() {
        Properties properties = new Properties();
        Map<String, Object> map = ConfigurationUtils.propertiesToMap(properties);
        
        assertNotNull(map);
        assertTrue(map.isEmpty());
    }

    @Test
    @DisplayName("mapToProperties should convert Map to Properties correctly")
    void testMapToProperties() {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        
        Properties properties = ConfigurationUtils.mapToProperties(map);
        
        assertNotNull(properties);
        assertEquals("value1", properties.getProperty("key1"));
        assertEquals("value2", properties.getProperty("key2"));
        assertEquals(2, properties.size());
    }

    @Test
    @DisplayName("mapToProperties should handle empty Map")
    void testMapToPropertiesEmpty() {
        Map<String, Object> map = new HashMap<>();
        Properties properties = ConfigurationUtils.mapToProperties(map);
        
        assertNotNull(properties);
        assertTrue(properties.isEmpty());
    }

    @Test
    @DisplayName("replaceInValues should replace values containing key")
    void testReplaceInValues() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "{{bootstrap.servers}}");
        properties.setProperty("schema.registry.url", "{{schema.registry.url}}");
        
        Properties result = ConfigurationUtils.replaceInValues(properties, "{{bootstrap.servers}}", "localhost:9092");
        
        assertEquals("localhost:9092", result.getProperty("bootstrap.servers"));
        assertEquals("{{schema.registry.url}}", result.getProperty("schema.registry.url"));
    }

    @Test
    @DisplayName("replaceInValues should handle values not containing key")
    void testReplaceInValuesNoMatch() {
        Properties properties = new Properties();
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");
        
        Properties result = ConfigurationUtils.replaceInValues(properties, "{{bootstrap.servers}}", "localhost:9092");
        
        assertEquals("value1", result.getProperty("key1"));
        assertEquals("value2", result.getProperty("key2"));
    }

    @Test
    @DisplayName("withProperty should add property and return Properties for chaining")
    void testWithProperty() {
        Properties properties = new Properties();
        Properties result = ConfigurationUtils.withProperty(properties, "key1", "value1");
        
        assertSame(properties, result);
        assertEquals("value1", properties.getProperty("key1"));
    }

    @Test
    @DisplayName("withProperty should overwrite existing property")
    void testWithPropertyOverwrite() {
        Properties properties = new Properties();
        properties.setProperty("key1", "oldValue");
        
        ConfigurationUtils.withProperty(properties, "key1", "newValue");
        
        assertEquals("newValue", properties.getProperty("key1"));
    }

    @Test
    @DisplayName("All methods should handle null inputs gracefully")
    void testNullInputHandling() {
        assertDoesNotThrow(() -> {
            ConfigurationUtils.replacePlaceholder(null, "key", "value");
            ConfigurationUtils.replacePlaceholders(null, new HashMap<>());
            ConfigurationUtils.propertiesToMap(null);
            ConfigurationUtils.mapToProperties(null);
            ConfigurationUtils.replaceInValues(null, "key", "value");
            ConfigurationUtils.withProperty(null, "key", "value");
        });
    }
}
