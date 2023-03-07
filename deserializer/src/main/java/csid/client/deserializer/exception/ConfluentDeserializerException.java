/*
 * -
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

package csid.client.deserializer.exception;

public class ConfluentDeserializerException extends RuntimeException {
    public ConfluentDeserializerException(String message) {
        super(message);
    }
    public ConfluentDeserializerException(String message, Exception innerException) {
        super(message, innerException);
    }
}
