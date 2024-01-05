/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package io.confluent.common.test.utils;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Assertions;

public class KafkaConsumer<K, V> implements Closeable {

  private static final int MAX_RECORDS = Integer.MAX_VALUE;
  private static final int MAX_CONSUME_DURATION = 30000;
  private final org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer;

  public KafkaConsumer(final String propertiesPath) throws IOException {
    this(ExtendedProperties.loadFromResource(propertiesPath, KafkaConsumer.class));
  }

  public KafkaConsumer(final Properties properties) {
    consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
    String topic = properties.getProperty("topic");
    if (topic != null) {
      consumer.subscribe(Collections.singletonList(topic));
    }
  }

  public KafkaConsumer(final Map<String, Object> conf) {
    consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(conf);
    String topic = (String) conf.get("topic");
    if (topic != null) {
      consumer.subscribe(Collections.singletonList(topic));
    }
  }

  public KafkaConsumer(
      final Properties properties,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer) {
    consumer =
        new org.apache.kafka.clients.consumer.KafkaConsumer<>(
            properties, keyDeserializer, valueDeserializer);
    String topic = properties.getProperty("topic");
    if (topic != null) {
      consumer.subscribe(Collections.singletonList(topic));
    }
  }

  public KafkaConsumer(
      final Map<String, Object> conf,
      final Deserializer<K> keyDeserializer,
      final Deserializer<V> valueDeserializer) {
    consumer =
        new org.apache.kafka.clients.consumer.KafkaConsumer<>(
            conf, keyDeserializer, valueDeserializer);
    String topic = (String) conf.get("topic");
    if (topic != null) {
      consumer.subscribe(Collections.singletonList(topic));
    }
  }

  public KafkaConsumer<K, V> withTopic(String topic) {
    consumer.subscribe(Collections.singletonList(topic));
    return this;
  }

  public List<ConsumerRecord<K, V>> consumeRecords(int minCount, int maxCount) {
    List<ConsumerRecord<K, V>> consumedRecords = new ArrayList<>();
    int received = 0;
    long endTimeMs = System.currentTimeMillis() + 40000;
    while (received < minCount && System.currentTimeMillis() < endTimeMs) {
      ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
      received += records.count();
      for (ConsumerRecord<K, V> record : records) {
        consumedRecords.add(record);
      }
    }
    Assertions.assertTrue(
        received >= minCount,
        "Some messages not consumed: min=" + minCount + ", received=" + received);
    Assertions.assertTrue(
        received <= maxCount,
        "Too many messages consumed: max=" + maxCount + ", received=" + received);
    return consumedRecords;
  }

  public Map<K, V> consumeUnique() {
    return consumeUnique(MAX_RECORDS);
  }

  public Map<K, V> consumeUnique(int consumeAmount) {
    return consumeUnique(consumeAmount, MAX_CONSUME_DURATION);
  }

  public Map<K, V> consumeUnique(int consumeAmount, int maxDurationMs) {
    Map<K, V> consumedRecords = new HashMap<>();
    long endTimeMs = System.currentTimeMillis() + maxDurationMs;
    int received = 0;
    while (received < consumeAmount && System.currentTimeMillis() < endTimeMs) {
      ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
      received += records.count();
      for (ConsumerRecord<K, V> record : records) {
        consumedRecords.put(record.key(), record.value());
      }
    }
    return consumedRecords;
  }

  @SuppressWarnings("unused")
  public void consume(int milliseconds, Consumer<K, V> callback) {
    ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(milliseconds / 10));
    records.forEach(record -> callback.onConsume(record.key(), record.value()));
    consumer.commitSync();
  }

  public void close() {
    consumer.unsubscribe();
    consumer.close();
  }

  public interface Consumer<K, V> {
    void onConsume(final K key, final V value);
  }
}
