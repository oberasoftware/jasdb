package com.obera.service.acl;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.service.StorageService;

/**
 * @author Renze de Vries
 */
public interface AuthorizationOperation {
    void doOperation(AuthorizationServiceWrapper authorizationServiceWrapper,
                     StorageService wrappedService,
                     String user,
                     String password) throws JasDBStorageException;
}
