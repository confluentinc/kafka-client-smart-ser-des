/*
 * -
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

package csid.client.schema;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * This class is used to create a {@link AbstractKafkaSchemaSerDeConfig} object.
 */
public class SchemaRegistryClientConfig extends AbstractKafkaSchemaSerDeConfig {

    private static final ConfigDef config;

    static {
        config = baseConfigDef();
    }

    public SchemaRegistryClientConfig(Map<?, ?> props) {
        super(config, props);
    }

}
