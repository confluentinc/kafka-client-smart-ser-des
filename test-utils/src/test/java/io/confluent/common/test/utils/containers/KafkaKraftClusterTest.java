/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package io.confluent.common.test.utils.containers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import io.confluent.common.test.utils.KafkaConsumer;
import io.confluent.common.test.utils.KafkaProducer;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

class KafkaKraftClusterTest {

    @Test
    void testKafkaKraftClusterStarted() {
        try (KafkaKraftCluster kafkaKraftCluster = KafkaKraftCluster.defaultCluster()) {
            kafkaKraftCluster.start();
            // assert that size is 1
            assertThat(kafkaKraftCluster.getKafkaContainers()).hasSize(1);

            String brokers = kafkaKraftCluster.getBootstrapServers();
            testKafkaFunctionality(brokers, 1, 1);
        }
    }

    @SneakyThrows
    void testKafkaFunctionality(String bootstrapServers, int partitions, int replicationFactor) {
        try (AdminClient adminClient =
                     AdminClient.create(
                             ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
             KafkaConsumer<String, String> kafkaConsumer =
                     new KafkaConsumer<>(
                             ImmutableMap.of(
                                     ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                     bootstrapServers,
                                     ConsumerConfig.GROUP_ID_CONFIG,
                                     "test-group-id-" + UUID.randomUUID(),
                                     ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                     "earliest",
                                     ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                                     StringDeserializer.class.getName(),
                                     ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                                     StringDeserializer.class.getName()));
             KafkaProducer<String, String> kafkaProducer =
                     new KafkaProducer<>(
                             ImmutableMap.of(
                                     ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                     bootstrapServers,
                                     ProducerConfig.CLIENT_ID_CONFIG,
                                     "test-client-id-" + UUID.randomUUID(),
                                     ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                     StringSerializer.class.getName(),
                                     ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                     StringSerializer.class.getName()))) {

            String topicName = "test-topic";

            Collection<NewTopic> topics =
                    Collections.singletonList(new NewTopic(topicName, partitions, (short) replicationFactor));

            // create topic and assert that it is created
            adminClient.createTopics(topics).all().get();

            await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(
                            () -> {
                                Set<String> createdTopics = adminClient.listTopics().names().get();
                                return createdTopics.contains(topicName);
                            });

            kafkaProducer.withTopic(topicName);
            kafkaProducer.produce("k1", "v1").get();

            kafkaConsumer.withTopic(topicName);
            Map<String, String> records = kafkaConsumer.consumeUnique(1);

            assertThat(records).containsEntry("k1", "v1");
        }
    }

    @Test
    void testKafkaKraftCluster_with_MultipleBrokers() {
        try (KafkaKraftCluster kafkaKraftCluster = KafkaKraftCluster.newCluster().withNumBrokers(3)) {
            kafkaKraftCluster.start();
            // assert that size is 3
            assertThat(kafkaKraftCluster.getKafkaContainers()).hasSize(3);

            String brokers = kafkaKraftCluster.getBootstrapServers();
            testKafkaFunctionality(brokers, 3, 2);
        }
    }

    @SneakyThrows
    @Test
    void testKafkaKraftCluster_with_MultipleBrokers_and_CustomConfig() {
        try (KafkaKraftCluster kafkaKraftCluster =
                     KafkaKraftCluster.newCluster()
                             .withNumBrokers(3)
                             .withContainerEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")) {
            kafkaKraftCluster.start();
            // assert that size is 3
            assertThat(kafkaKraftCluster.getKafkaContainers()).hasSize(3);

            String brokers = kafkaKraftCluster.getBootstrapServers();
            testKafkaFunctionality(brokers, 3, 2);

            // assert that the offsets topic has a replication factor of 1
            try (AdminClient adminClient =
                         AdminClient.create(
                                 ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokers))) {
                ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
                Set<String> topics =
                        adminClient.listTopics(listTopicsOptions.listInternal(true)).names().get();

                TopicDescription consumerOffsetsDescription =
                        adminClient
                                .describeTopics(Collections.singletonList("__consumer_offsets"))
                                .allTopicNames()
                                .get()
                                .get("__consumer_offsets");
                assertThat(consumerOffsetsDescription.partitions().get(0).replicas()).hasSize(1);
            }
        }
    }

    @Test
    void testKafkaKraftCluster_with_CapturedContainerLogs() {
        try (KafkaKraftCluster kafkaKraftCluster = KafkaKraftCluster.defaultCluster()) {
            kafkaKraftCluster.start();
            // assert that size is 1
            assertThat(kafkaKraftCluster.getKafkaContainers()).hasSize(1);

            String brokers = kafkaKraftCluster.getBootstrapServers();
            testKafkaFunctionality(brokers, 1, 1);

            String containerLogs = kafkaKraftCluster.getKafkaContainerLogs();
            // assert that logs are captured
            assertThat(containerLogs).contains("Kafka Server started (kafka.server.KafkaRaftServer)");
        }
    }

    @Test
    void testKafkaKraftCluster_ServiceHost_and_ServicePort() {
        try (KafkaKraftCluster kafkaKraftCluster = KafkaKraftCluster.newCluster().withNumBrokers(3)) {
            kafkaKraftCluster.start();
            // assert that size is 1
            assertThat(kafkaKraftCluster.getKafkaContainers()).hasSize(3);

            String brokers = kafkaKraftCluster.getBootstrapServers();
            testKafkaFunctionality(brokers, 1, 1);

            String serviceHost = kafkaKraftCluster.getServiceHost("broker-1", 9093);
            int servicePort = kafkaKraftCluster.getServicePort("broker-1", 9093);

            assertThat(serviceHost).isNotEqualTo("broker-1");
            assertThat(servicePort).isNotEqualTo(9093);
        }
    }

    @Test
    void testKafkaKraftCluster_with_SchemaRegistry() {
        try (KafkaKraftCluster kafkaKraftCluster =
                     KafkaKraftCluster.defaultCluster().withSchemaRegistry()) {
            kafkaKraftCluster.start();
            assertThat(kafkaKraftCluster.getClusterSize()).isEqualTo(1);
            assertThat(kafkaKraftCluster.getSchemaRegistryContainer()).isNotNull();
            assertThat(kafkaKraftCluster.isSchemaRegistryRunning()).isTrue();
        }
    }
}
