package com.oberasoftware.jasdb.api.exceptions;

/**
 * @author Renze de Vries
 */
public class JasDBSecurityException extends JasDBStorageException {
    public JasDBSecurityException(String message) {
        super(message);
    }

    public JasDBSecurityException(String message, Throwable e) {
        super(message, e);
    }
}
