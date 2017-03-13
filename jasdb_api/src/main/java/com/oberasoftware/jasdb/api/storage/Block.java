package com.oberasoftware.jasdb.api.storage;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface Block {
    long getPosition();

    void close() throws JasDBStorageException;
}
