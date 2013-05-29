package nl.renarj.jasdb.storage.exceptions;

import nl.renarj.jasdb.core.exceptions.DatastoreException;

/**
 * Thrown when a record is not found during retrieval operation
 *
 * @author Renze de Vries
 */
public class RecordNotFoundException extends DatastoreException {
    public RecordNotFoundException(String message) {
        super(message);
    }
}
