# Kafka Client Smart Serializer / Deserializer

## Overview
This repository contains the smart serializer and deserializer for Apache Kafka® clients.

The smart serializer and deserializer allows for engineers to create consumers and producers without knowing the
underlying stream contents.

If a low dependency model is required, it is recommended to review usage of required serializer and deserializer libraries as outlined [here](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html), as this repository includes all serializers and deserializers by default.

Here is a talk from [Current 24](https://current.confluent.io/2024-sessions/simplifying-apache-kafka-r-easy-configurations-for-immediate-impact) where the library was presented.

## Table of Contents

- [Kafka Client Smart Serializer / Deserializer](#kafka-client-smart-serializer--deserializer)
  - [Overview](#overview)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Maven](#maven)
    - [Producer](#producer)
    - [Consumer](#consumer)
  - [Supported data types](#supported-data-types)
    - [SerializationTypes](#serializationtypes)
  - [Contributing](#contributing)
    - [Getting Started](#getting-started)
    - [Development Setup](#development-setup)
    - [Making Changes](#making-changes)
    - [Submitting Changes](#submitting-changes)
    - [Code Standards](#code-standards)
    - [Types of Contributions](#types-of-contributions)
    - [Questions or Issues?](#questions-or-issues)
    - [Code of Conduct](#code-of-conduct)

## Installation

### Maven

Deserializer
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-client-smart-deserializer</artifactId>
    <version>${kafka-client-smart-ser-des.version}</version>
</dependency>
```

Serializer
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-client-smart-serializer</artifactId>
    <version>${kafka-client-smart-ser-des.version}</version>
</dependency>
```

Common (Schema Registry Client Config and Utils)
```xml
 <dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-client-smart-common</artifactId>
    <version>${kafka-client-smart-ser-des.version}</version>
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
Kafka Consumer Initialization
```java
 try(KafkaConsumer<K, V> consumer=new KafkaConsumer<>(
        properties,
        new ConfluentDeserializer<>(properties,true,kClass),
        new ConfluentDeserializer<V>(properties,false,vClass))){
 ```

## Supported data types

| Primitive Types | Serializer Docs                                                                                                                                   | Deserializer Docs                                                                                                                                     |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| String          | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/StringSerializer.html)     | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/StringDeserializer.html)     |
| Boolean         | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BooleanSerializer.html)    | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BooleanDeserializer.html)    |
| Float           | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/FloatSerializer.html)      | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/FloatDeserializer.html)      |
| Double          | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/DoubleSerializer.html)     | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/DoubleDeserializer.html)     |
| Integer         | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/IntegerSerializer.html)    | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/IntegerDeserializer.html)    |
| Long            | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/LongSerializer.html)       | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/LongDeserializer.html)       |
| Short           | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ShortSerializer.html)      | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ShortDeserializer.html)      |
| Bytes           | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BytesSerializer.html)      | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/BytesDeserializer.html)      |
| ByteArray       | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteArraySerializer.html)  | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteArrayDeserializer.html)  |
| ByteBuffer      | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteBufferSerializer.html) | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/ByteBufferDeserializer.html) |
| UUID            | [Serializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/UUIDSerializer.html)       | [Deserializer](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/org/apache/kafka/common/serialization/UUIDDeserializer.html)       |

| Schemaless Types| Serializer Docs | Deserializer Docs |
|-----------------|-----------------|-------------------|
| KafkaJSON      | [Serializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonSerializer.java) | [Deserializer](https://github.com/confluentinc/schema-registry/blob/master/json-serializer/src/main/java/io/confluent/kafka/serializers/KafkaJsonDeserializer.java) |

| Schema Types   | Serializer Docs | Deserializer Docs |
|----------------|------------|--------------|
| KafkaAvro      | [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#avro-serializer)        | [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-avro.html#avro-deserializer)    |
| KafkaJSONSchema| [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-serializer) | [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-json.html#json-schema-deserializer) |
| Protobuf       | [Serializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#protobuf-serializer) | [Deserializer](https://docs.confluent.io/platform/current/schema-registry/serdes-develop/serdes-protobuf.html#protobuf-deserializer) |

### SerializationTypes

The [`SerializationTypes.class`](common/src/main/java/confluent/client/common/SerializationTypes.java) is used to determine the type of data type that needs to be serialized/deserialized via a range of methods:

1. #### Headers
This method is used to determine the serialization type from the headers and compared against an Enum matching the data type via `valueOf`

2. #### String
This method is used to determine the serialization type from a string and compared against an Enum matching the data type via `valueOf`

3. #### Bytes
This method is used to determine the serialization type of the message via the comparison of bytes. (Primitives are checked first then Schema)

| Primitive Types  | Bytes Check                                                         |
|------------------|---------------------------------------------------------------------|
| String (Default) | `bytes.length == 0` (edge case of possible empty string "")         |
| Boolean          | `bytes.length == 1 && if (bytes[0] == 0x00 ` OR `bytes[0] == 0x01)` |
| ByteArray        | `bytes.length == 1`                                                 |
| Short            | `bytes.length == 2`                                                 | 
| Integer          | `bytes.length == 4`                                                 |
| Long             | `bytes.length == 8`                                                 |
|                  |                                                                     |

| Schemaless Types | Bytes Check                                                                                                     |
|------------------|-----------------------------------------------------------------------------------------------------------------|
| JSON             | `(bytes[0] == '{' && bytes[bytes.length - 1] == '}')` OR `(bytes[0] == '[' && bytes[bytes.length - 1] == ']'))` |

**Remaining Schema Types are compared in the Schema Check**

4. #### Schema
This method is used to determine the serialization type of the message via the use of the
Schema Registry supplier.

| Schema Types    | Schema Type |
|-----------------|-------------|
| Avro            | `AVRO`      |
| KafkaJSONSchema | `JSON`      |
| Protobuf        | `PROTOBUF`  |

5. #### Class
```mermaid
stateDiagram-v2
    state if_state <<choice>>
    state fork_state <<fork>>
    [*] --> if_state
    if_state --> PrimitiveTypes: bytes <=8
    if_state --> fork_state: bytes > 8
    fork_state --> SchemaRegistryTypes: hasMagicByte
    fork_state --> SchemalessTypes
    state SchemalessTypes {
        state is_json <<choice>>
        is_json --> json: if '{...}' or '[...]'
        is_json --> string: else
    }

```

## Contributing

We welcome contributions to the Kafka Client Smart Serializer/Deserializer project! Here's how you can get involved:

### Getting Started

1. **Clone your fork** locally:
   ```
   git clone https://github.com/your-username/kafka-client-smart-ser-des.git
   cd kafka-client-smart-ser-des
   ```

### Development Setup

1. **Install dependencies**:
   ```
   mvn clean install
   ```

2. **Run tests** to ensure everything works:
   ```
   mvn clean test
   ```

3. **Run integration tests** (if applicable):
   ```
   mvn verify
   ```

### Making Changes

1. **Create a feature branch**:
   ```
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes** following these guidelines:
   - Follow the existing code style and conventions
   - Add appropriate unit tests for new functionality
   - Update documentation as needed
   - Ensure all tests pass

3. **Commit your changes** with clear, descriptive commit messages:
   ```
   git add .
   git commit -m "Add feature: brief description of changes"
   ```

### Submitting Changes

1. **Push your branch** to your fork:
   ```
   git push origin feature/your-feature-name
   ```

2. **Create a Pull Request** on GitHub with:
   - A clear title and description
   - Reference to any related issues
   - Screenshots or examples if applicable

### Code Standards

- **Java**: Follow standard Java conventions and the existing code style
- **Testing**: Maintain or improve test coverage
- **Documentation**: Update README and other docs as needed
- **Backwards Compatibility**: Consider impact on existing users

### Types of Contributions

We welcome various types of contributions:

-  **Bug fixes**
-  **New features**
-  **Documentation improvements**
-  **Test enhancements**
-  **Performance optimizations**
-  **Code refactoring**

### Questions or Issues?

- **Bug reports**: Use GitHub Issues with detailed reproduction steps
- **Feature requests**: Use GitHub Issues with clear use cases

### Code of Conduct

Please be respectful and constructive in all interactions. We aim to create a welcoming environment for all contributors.

Thank you for contributing to the Kafka Client Smart Serializer/Deserializer project! 
