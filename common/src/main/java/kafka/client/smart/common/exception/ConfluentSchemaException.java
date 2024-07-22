/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.common.exception;

import org.apache.kafka.common.KafkaException;

public class ConfluentSchemaException extends KafkaException {
    public ConfluentSchemaException(String message) {
        super(message);
    }

    public ConfluentSchemaException(String message, Exception innerException) {
        super(message, innerException);
    }
}
