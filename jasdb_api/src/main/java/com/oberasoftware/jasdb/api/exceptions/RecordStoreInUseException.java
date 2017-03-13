package com.oberasoftware.jasdb.api.exceptions;

/**
 * @author Renze de Vries
 */
public class RecordStoreInUseException extends DatastoreException {
    public RecordStoreInUseException(String message) {
        super(message);
    }
}
