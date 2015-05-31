package nl.renarj.jasdb.storage.exceptions;

import nl.renarj.jasdb.core.exceptions.RuntimeJasDBException;

/**
 * Thrown when a record is not found during retrieval operation
 *
 * @author Renze de Vries
 */
public class RecordNotFoundException extends RuntimeJasDBException {
    public RecordNotFoundException(String message) {
        super(message);
    }
}
