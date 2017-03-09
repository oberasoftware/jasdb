package com.oberasoftware.jasdb.acl;

import com.oberasoftware.jasdb.engine.StorageService;

/**
 * @author Renze de Vries
 */
public interface AuthorizationOperation {
    void doOperation(StorageService wrappedService,
                     String user,
                     String password) throws Exception;
}
