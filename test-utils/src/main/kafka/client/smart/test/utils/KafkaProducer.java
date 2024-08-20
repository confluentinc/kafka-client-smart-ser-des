/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.common.test.utils;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaProducer<K, V> implements Closeable {

  private final org.apache.kafka.clients.producer.KafkaProducer<K, V> producer;
  private String topic;

  public KafkaProducer(final String propertiesPath) throws IOException {
    this(ExtendedProperties.loadFromResource(propertiesPath, KafkaProducer.class));
  }

  public KafkaProducer(final Properties properties) {
    topic = properties.getProperty("topic");
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
  }

  public KafkaProducer(final Map<String, Object> conf) {
    topic = (String) conf.get("topic");
    producer = new org.apache.kafka.clients.producer.KafkaProducer<>(conf);
  }

  public KafkaProducer(
      final Properties properties,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer) {
    producer =
        new org.apache.kafka.clients.producer.KafkaProducer<>(
            properties, keySerializer, valueSerializer);
    topic = properties.getProperty("topic");
  }

  public KafkaProducer(
      final Map<String, Object> conf,
      final Serializer<K> keySerializer,
      final Serializer<V> valueSerializer) {
    producer =
        new org.apache.kafka.clients.producer.KafkaProducer<>(conf, keySerializer, valueSerializer);
    topic = (String) conf.get("topic");
  }

  public KafkaProducer<K, V> withTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public Future<RecordMetadata> produce(final K key, final V value) {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
    producer.flush();
    return future;
  }

  public Future<RecordMetadata> produceAsync(final K key, final V value) {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, key, value));
    return future;
  }

  public void close() {
    producer.flush();
    producer.close();
  }

  public void close(Duration timeout) {
    producer.flush();
    producer.close(timeout);
  }
}
