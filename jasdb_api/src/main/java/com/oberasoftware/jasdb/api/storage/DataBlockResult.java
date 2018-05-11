package com.oberasoftware.jasdb.api.storage;

/**
 * @author Renze de Vries
 */
public interface DataBlockResult<T> {
    int getNextOffset();

    long getDataLength();

    DataBlock getEndBlock();

    T getValue();
}
