
package csid.client.common.exception;

public class ConfluentSchemaException extends RuntimeException {
    public ConfluentSchemaException(String message) {
        super(message);
    }

    public ConfluentSchemaException(String message, Exception innerException) {
        super(message, innerException);
    }
}
