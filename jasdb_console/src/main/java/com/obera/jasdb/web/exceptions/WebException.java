package com.obera.jasdb.web.exceptions;

import nl.renarj.jasdb.core.exceptions.JasDBException;

/**
 * @author renarj
 */
public class WebException extends JasDBException {
    public WebException(String message) {
        super(message);
    }
}
