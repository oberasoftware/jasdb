package com.oberasoftware.jasdb.api.storage;

/**
 * @author renarj
 */
public interface DataBlockResult<T> {
    int getNextOffset();

    long getDataLength();

    DataBlock getEndBlock();

    T getValue();
}
