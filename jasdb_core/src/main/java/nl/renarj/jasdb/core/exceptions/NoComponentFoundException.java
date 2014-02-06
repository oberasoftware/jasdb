package nl.renarj.jasdb.core.exceptions;

/**
 * @author renarj
 */
public class NoComponentFoundException extends JasDBStorageException {
    public NoComponentFoundException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
