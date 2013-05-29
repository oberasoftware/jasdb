package nl.renarj.jasdb.service;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 10:43 AM
 */
public interface IdGenerator {
    public String generateNewId() throws JasDBStorageException;
}
