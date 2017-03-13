package com.oberasoftware.jasdb.rest.client;

import nl.renarj.jasdb.remote.exceptions.RemoteException;

/**
 * @author Renze de Vries
 *         Date: 8-6-12
 *         Time: 19:05
 */
public class ResourceNotFoundException extends RemoteException {
    public ResourceNotFoundException(String message) {
        super(message);
    }
}
