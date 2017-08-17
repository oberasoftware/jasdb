package com.oberasoftware.jasdb.console.exceptions;


import com.oberasoftware.jasdb.api.exceptions.JasDBException;

/**
 * @author renarj
 */
public class WebException extends JasDBException {
    public WebException(String message) {
        super(message);
    }
}
