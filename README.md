# CSID Smart Serializer / Deserializer

## Overview
This repository contains the smart serializer and deserializer for kafka connectors.

The smart serializer and deserializer allows for engineers to create consumers and producers without knowing the
underlying stream contents.


## Table Of Content

- [Installation](#Installation)
- [Supported data types](#Supported-data-types)

## Installation

### Maven

Deserializer
```xml
<dependency>
    <groupId>io.confluent.csid</groupId>
    <artifactId>csid-confluent-deserializer</artifactId>
    <version>${csid-confluent-ser-des.version}</version>
</dependency>
```

Serializer
```xml
<dependency>
    <groupId>io.confluent.csid</groupId>
    <artifactId>csid-confluent-serializer</artifactId>
    <version>${csid-confluent-ser-des.version}</version>
</dependency>
```

Common (Schema Registry Client Config and Utils)
```xml
 <dependency>
    <groupId>io.confluent.csid</groupId>
    <artifactId>csid-confluent-ser-des-common</artifactId>
    <version>${csid-confluent-ser-des.version}</version>
</dependency>
```
### Producer

Kafka Producer Initialization

```Java
    KafkaProducer<K, V> producer=new KafkaProducer<>(properties,
        new ConfluentSerializer<>(properties,true),
        new ConfluentSerializer<>(properties,false));
```
### Consumer
Kafka Producer Initialization
```java
 try(KafkaConsumer<K, V> consumer=new KafkaConsumer<>(
        properties,
        new ConfluentDeserializer<>(properties,true,kClass),
        new ConfluentDeserializer<V>(properties,false,vClass))){
 ```

#### Supported data types

- String - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/StringSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html)

- Float - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/FloatSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/FloatDeserializer.html)

- Double - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/DoubleSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/DoubleDeserializer.html)

- Integer - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/IntegerSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/IntegerDeserializer.html)

- Long - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/LongSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/LongDeserializer.html)

- Short - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ShortSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ShortDeserializer.html)

- Bytes - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BytesSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BytesDeserializer.html)

- ByteArray - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteArraySerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteArrayDeserializer.html)

- ByteBuffer - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteBufferSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteBufferDeserializer.html)

- UUID - [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/UUIDSerializer.html)
  / [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/UUIDDeserializer.html)

- KafkaAvro - [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#avro-serializer) /
  [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#avro-deserializer)

- KafkaJSONSchema - [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer) /
  [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-deserializer)

- KafkaJSON - [Serializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonSerializer.java) /
  [Deserializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonDeserializer.java)

- Protobuf - [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#protobuf-serializer) /
  [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#protobuf-deserializer)

