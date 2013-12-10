package nl.renarj.jasdb.api.model;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface IndexManagerFactory {
    IndexManager getIndexManager(String instanceId) throws JasDBStorageException;
}
