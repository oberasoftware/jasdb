package com.oberasoftware.jasdb.api.exceptions;

/**
 * @author renarj
 */
public class LockingException extends RuntimeException {
    public LockingException(String message) {
        super(message);
    }
}
