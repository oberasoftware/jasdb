package com.oberasoftware.jasdb.api.exceptions;

/**
 * @author Renze de Vries
 */
public class LockingException extends RuntimeException {
    public LockingException(String message) {
        super(message);
    }
}
