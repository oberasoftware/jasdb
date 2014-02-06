/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.core.exceptions;

/**
 * User: renarj
 * Date: 2/1/12
 * Time: 8:29 PM
 */
public class ServiceException extends JasDBException {
    public ServiceException(String message) {
        super(message);
    }
    
    public ServiceException(String message, Throwable e) {
        super(message, e);
    }
}
