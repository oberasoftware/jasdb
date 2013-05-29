package nl.renarj.jasdb.storage.exceptions;

import nl.renarj.jasdb.core.exceptions.DatastoreException;

/**
 * @author Renze de Vries
 */
public class RecordStoreInUseException extends DatastoreException {
    public RecordStoreInUseException(String message) {
        super(message);
    }
}
