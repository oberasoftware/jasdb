package com.oberasoftware.jasdb.api.storage;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface DataBlockHeader {
    static final int HEADER_SIZE = 100;

    long getNext();

    long getPrevious();

    void setNext(long next);

    void setPrevious(long previous);

    int marker();

    int markerWithHeader();

    void incrementMarker(int amount);

    void resetMarker();

    void putLong(int offset, long value) throws JasDBStorageException;

    void putInt(int offset, int value) throws JasDBStorageException;

    long getLong(int offset) throws JasDBStorageException;

    int getInt(int offset) throws JasDBStorageException;

    void setNextStream(long nextStreamLocation);

    long getNextStream();
}
