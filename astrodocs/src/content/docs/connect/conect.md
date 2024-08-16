---
title: Kafka Connect With Confluent Value Converter Documentation
tableOfContents:
  maxHeadingLevel: 4
---
## Introduction

Kafka Connect is a framework in Apache Kafka® that allows you to easily integrate external systems with Kafka. It provides a scalable and reliable way to stream data between Kafka topics and external data sources or sinks.
Confluent Value Converter is a Kafka Connect converter that allows you to serialize and deserialize data in Avro, JSON, and Protobuf formats. It provides a flexible and efficient way to convert data between different formats.
It is also possible to consume multiple data formats in the same Kafka topic. For example, you can consume a JSON message from the same Kafka topic as an Avro message.

This documentation will guide you through the setup and configuration of Kafka Connect with Confluent Value Converter, as well as provide examples of common use cases and best practices.

## Table of Contents

- Installation
- Configuration
- Connectors
- Working with Source Connectors
- Working with Sink Connectors
- Best Practices

## Installation

Kafka Connect is included in the Apache Kafka distribution, so you can install it by following the Kafka installation instructions. Make sure to download the appropriate version of Kafka that matches your environment.

Once Kafka is installed, Kafka Connect is available as a separate component that can be started using the `connect-distributed.sh` script.

## Configuration

Kafka Connect uses configuration files to define the behavior of connectors and workers. The main configuration file is `connect-distributed.properties`, located in the `config` directory of your Kafka installation.

Here are some important configuration properties you should be aware of:

- `bootstrap.servers`: Specifies the list of Kafka brokers that Kafka Connect should connect to.
- `group.id`: The unique identifier for the group of workers running Kafka Connect. Each group should have a unique ID to avoid conflicts.
- `key.converter` and `value.converter`: Specifies the converter classes for serializing and deserializing data. It should be set to `kafka.client.smart.connect.ConfluentValueConverter`.
- `offset.storage.topic`: The Kafka topic where Kafka Connect stores its offset information. By default, this is set to `__connect-offsets`.
- `plugin.path`: The directory where connector plugins are located. By default, this is set to `./plugins`.
- `key.converter.type` and `value.converter.type`: Specifies the type of data format to use for serialization (source connector). The default is set to `Avro`.

Confluent Value Converter automatically uses the proper data format during deserialization (sink connector) and  supports the following data formats:
* `Avro` - Avro format
* `Protobuf` - Protobuf format
* `JsonSchema` - JSON Schema format
* `Json` - JSON format
* `String` - String format
* `ByteArray` - Byte Array format
* `Short` - Short format
* `Integer` - Integer format
* `Long` - Long format
* `Float` - Float format
* `Double` - Double format

Make sure to configure these properties according to your environment and requirements. You can find a full list of configuration properties in the Kafka documentation.

## Connectors

Connectors are the building blocks of Kafka Connect. They define the integration between Kafka and external systems, allowing you to either stream data from Kafka to an external system (source connector) or stream data from an external system to Kafka (sink connector).

Kafka Connect provides a variety of built-in connectors, such as the JDBC source connector for databases, HDFS sink connector for Hadoop, and more. Additionally, you can develop custom connectors to integrate with your specific systems.

## Working with Source Connectors

Source connectors enable you to stream data from external systems into Kafka. Here's an example of how to use the JDBC source connector to stream data from a MySQL database:

1. Make sure the JDBC driver for MySQL is available in the connector's classpath.

2. Create a configuration file, e.g., `mysql-source.properties`, and specify the connector properties, including the database connection URL, username, password, topic name, etc.

```properties
name=mysql-source
connector.class=io.confluent.connect.jdbc.JdbcSourceConnector
tasks.max=1
connection.url=jdbc:mysql://localhost:3306/mydatabase
connection.user=myuser
connection.password=mypassword
topic.prefix=mysql-
value.converter=kafka.client.smart.connect.ConfluentValueConverter
```

3. Start Kafka Connect in distributed mode using the `connect-distributed.sh` script.

```bash
$ ./bin/connect-distributed.sh config/connect-distributed.properties mysql-source.properties
```

The JDBC source connector will continuously poll the MySQL database for new or updated data and write it to Kafka topics prefixed with "mysql-".

## Working with Sink Connectors

Sink connectors allow you to stream data from Kafka to external systems. Here's an example of how to use the HDFS sink connector to write data from Kafka to Hadoop HDFS:

1. Make sure Hadoop and HDFS are properly set up and accessible from the machine running Kafka Connect.

2. Create a configuration file, e.g., `hdfs-sink.properties`, and specify the connector properties, including the HDFS URL, output directory, Kafka topic, etc.

```properties
name=hdfs-sink
connector.class=io.confluent.connect.hdfs.HdfsSinkConnector
tasks.max=1
hdfs.url=hdfs://localhost:9000
hdfs.output=/data/kafka
topics=mytopic
value.converter=kafka.client.smart.connect.ConfluentValueConverter
```
3. Start Kafka Connect in distributed mode using the `connect-distributed.sh` script.

```bash
$ ./bin/connect-distributed.sh config/connect-distributed.properties hdfs-sink.properties
```

The HDFS sink connector will consume messages from the specified Kafka topic and write them to the specified HDFS directory.

## Best Practices

Here are some best practices to consider when working with Kafka Connect:

- Use a distributed mode of Kafka Connect for scalability and fault tolerance. It allows you to run multiple worker instances that can share the load and provide high availability.

- Configure proper data serialization and deserialization by choosing the appropriate converter classes for your data formats.

- Monitor the status and performance of your connectors and workers using the Kafka Connect REST API or a monitoring tool.

- Secure your Kafka Connect setup by enabling authentication and encryption to protect sensitive data.

- Regularly update Kafka Connect and its connectors to benefit from bug fixes, performance improvements, and new features.

For more detailed information, refer to the Kafka Connect documentation.
