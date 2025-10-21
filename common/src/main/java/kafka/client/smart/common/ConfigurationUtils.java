/*-
 * Copyright (C) 2022-2025 Confluent, Inc.
 */

package kafka.client.smart.common;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Centralized utility for configuration management.
 * This eliminates code duplication across modules by providing common configuration loading,
 * resource handling, and property management functionality.
 */
@Slf4j
public final class ConfigurationUtils {

    private ConfigurationUtils() {
        // Utility class
    }

    /**
     * Load properties from a resource file.
     *
     * @param resourceName The resource name (e.g., "/config.properties")
     * @param clazz The class to use for resource loading
     * @return Properties loaded from the resource
     * @throws IOException if the resource cannot be loaded
     */
    public static Properties loadFromResource(String resourceName, Class<?> clazz) throws IOException {
        Properties properties = new Properties();
        try (InputStream inputStream = clazz.getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            properties.load(inputStream);
        }
        return properties;
    }

    /**
     * Load properties from a file path.
     *
     * @param filePath The file path
     * @return Properties loaded from the file
     * @throws IOException if the file cannot be loaded
     */
    public static Properties loadFromFile(String filePath) throws IOException {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(filePath)) {
            properties.load(input);
        }
        return properties;
    }

    /**
     * Load properties from a file path, falling back to resource if file not found.
     *
     * @param filePath The file path to try first
     * @param resourceName The resource name to fall back to
     * @param clazz The class to use for resource loading
     * @return Properties loaded from file or resource
     * @throws IOException if neither file nor resource can be loaded
     */
    public static Properties loadFromFileOrResource(String filePath, String resourceName, Class<?> clazz) throws IOException {
        try {
            return loadFromFile(filePath);
        } catch (IOException e) {
            log.debug("File not found, falling back to resource: {}", filePath);
            return loadFromResource(resourceName, clazz);
        }
    }

    /**
     * Get resource content as a string.
     *
     * @param resourceName The resource name
     * @param clazz The class to use for resource loading
     * @return The resource content as a string
     * @throws IOException if the resource cannot be loaded
     */
    public static String getResourceAsString(String resourceName, Class<?> clazz) throws IOException {
        try (InputStream inputStream = clazz.getResourceAsStream(resourceName)) {
            if (inputStream == null) {
                throw new IOException("Resource not found: " + resourceName);
            }
            return new String(inputStream.readAllBytes());
        }
    }

    /**
     * Get resource as a Path, handling JAR resources by extracting to temp file.
     *
     * @param resourceName The resource name
     * @param clazz The class to use for resource loading
     * @return Path to the resource (or extracted temp file)
     * @throws IOException if the resource cannot be accessed
     */
    public static Path getResourcePath(String resourceName, Class<?> clazz) throws IOException {
        String path = clazz.getResource(resourceName).getFile();
        
        if (path.contains(".jar!")) {
            // Extract JAR resource to temp file
            try (InputStream stream = clazz.getResourceAsStream(resourceName)) {
                if (stream == null) {
                    throw new IOException("Resource not found: " + resourceName);
                }
                
                Path tempDir = Files.createTempDirectory("config-utils");
                Path tempFile = tempDir.resolve(UUID.randomUUID().toString());
                Files.copy(stream, tempFile);
                return tempFile;
            }
        }
        
        return Paths.get(path);
    }

    /**
     * Replace placeholders in a configuration string.
     *
     * @param config The configuration string
     * @param replacements Map of placeholder to replacement value
     * @return Configuration string with placeholders replaced
     */
    public static String replacePlaceholders(String config, Map<String, String> replacements) {
        String result = config;
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
            result = result.replace(entry.getKey(), entry.getValue());
        }
        return result;
    }

    /**
     * Replace a single placeholder in a configuration string.
     *
     * @param config The configuration string
     * @param placeholder The placeholder to replace (e.g., "{{bootstrap.servers}}")
     * @param value The replacement value
     * @return Configuration string with placeholder replaced
     */
    public static String replacePlaceholder(String config, String placeholder, String value) {
        return config.replace(placeholder, value);
    }

    /**
     * Create a properties map from a Properties object.
     *
     * @param properties The Properties object
     * @return Map representation of the properties
     */
    public static Map<String, Object> propertiesToMap(Properties properties) {
        Map<String, Object> map = new HashMap<>();
        properties.forEach((key, value) -> map.put(key.toString(), value));
        return map;
    }

    /**
     * Create a Properties object from a map.
     *
     * @param map The map to convert
     * @return Properties object
     */
    public static Properties mapToProperties(Map<String, Object> map) {
        Properties properties = new Properties();
        map.forEach((key, value) -> properties.setProperty(key, value.toString()));
        return properties;
    }

    /**
     * Replace all occurrences of a key in property values.
     *
     * @param properties The Properties object to modify
     * @param key The key to replace
     * @param value The replacement value
     * @return The modified Properties object
     */
    public static Properties replaceInValues(Properties properties, String key, String value) {
        Properties result = new Properties();
        properties.forEach((k, v) -> {
            String newValue = v.toString().replace(key, value);
            result.setProperty(k.toString(), newValue);
        });
        return result;
    }

    /**
     * Add a property to a Properties object and return it for chaining.
     *
     * @param properties The Properties object
     * @param key The property key
     * @param value The property value
     * @return The Properties object for chaining
     */
    public static Properties withProperty(Properties properties, String key, String value) {
        properties.setProperty(key, value);
        return properties;
    }
}
