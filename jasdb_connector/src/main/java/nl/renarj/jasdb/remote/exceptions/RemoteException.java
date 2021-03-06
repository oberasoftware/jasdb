/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.remote.exceptions;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * User: renarj
 * Date: 1/22/12
 * Time: 3:26 PM
 */
public class RemoteException extends JasDBStorageException {
    public RemoteException(String message, Throwable e) {
        super(message, e);
    }
    
    public RemoteException(String message) {
        super(message);
    }
}
