package com.oberasoftware.jasdb.api.exceptions;

/**
 * @author Renze de Vries
 */
public class RuntimeJasDBException extends RuntimeException {
    public RuntimeJasDBException(String message, Throwable e) {
        super(message, e);
    }

    public RuntimeJasDBException(String message) {
        super(message);
    }
}
