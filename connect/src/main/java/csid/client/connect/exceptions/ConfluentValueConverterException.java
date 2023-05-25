package csid.client.connect.exceptions;

public class ConfluentValueConverterException extends RuntimeException{
    public ConfluentValueConverterException(String message) {
        super(message);
    }

    public ConfluentValueConverterException(String message, Exception innerException) {
        super(message, innerException);
    }

}
