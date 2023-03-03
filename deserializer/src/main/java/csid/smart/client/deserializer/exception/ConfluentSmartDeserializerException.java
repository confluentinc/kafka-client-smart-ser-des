/*
 * -
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

package csid.smart.client.deserializer.exception;

public class ConfluentSmartDeserializerException extends RuntimeException {
    public ConfluentSmartDeserializerException(String message) {
        super(message);
    }
    public ConfluentSmartDeserializerException(String message, Exception innerException) {
        super(message, innerException);
    }
}
