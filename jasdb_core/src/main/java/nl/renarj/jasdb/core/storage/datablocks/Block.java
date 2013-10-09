package nl.renarj.jasdb.core.storage.datablocks;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface Block {
    long getPosition();

    void close() throws JasDBStorageException;
}
