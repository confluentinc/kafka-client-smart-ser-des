
package exception;

public class ConfluentSmartSchemaException extends RuntimeException {
    public ConfluentSmartSchemaException(String message) {
        super(message);
    }
    public ConfluentSmartSchemaException(String message, Exception innerException) {
        super(message, innerException);
    }
}
