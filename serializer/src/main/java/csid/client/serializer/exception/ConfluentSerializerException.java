package csid.client.serializer.exception;

public class ConfluentSerializerException extends RuntimeException {
    public ConfluentSerializerException(String message) {
        super(message);
    }
    public ConfluentSerializerException(String message, Exception innerException) {
        super(message, innerException);
    }
}
