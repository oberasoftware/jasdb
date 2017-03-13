package com.oberasoftware.jasdb.api.exceptions;

import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;

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
