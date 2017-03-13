package com.oberasoftware.jasdb.api.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface IndexManagerFactory {
    IndexManager getIndexManager(String instanceId) throws JasDBStorageException;

    void shutdownIndexes() throws JasDBStorageException;
}
