package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * User: renarj
 * Date: 2/10/12
 * Time: 10:43 AM
 */
public interface IdGenerator {
    public String generateNewId() throws JasDBStorageException;
}
