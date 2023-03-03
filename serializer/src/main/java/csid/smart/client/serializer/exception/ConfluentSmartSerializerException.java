package csid.smart.client.serializer.exception;

public class ConfluentSmartSerializerException extends RuntimeException {
    public ConfluentSmartSerializerException(String message) {
        super(message);
    }
    public ConfluentSmartSerializerException(String message, Exception innerException) {
        super(message, innerException);
    }
}
