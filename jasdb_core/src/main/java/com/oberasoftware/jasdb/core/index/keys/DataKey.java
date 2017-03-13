package com.oberasoftware.jasdb.core.index.keys;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.exceptions.RuntimeJasDBException;
import com.oberasoftware.jasdb.core.storage.ClonableByteArrayInputStream;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.api.index.keys.CompareMethod;
import com.oberasoftware.jasdb.api.index.keys.CompareResult;
import com.oberasoftware.jasdb.api.index.keys.Key;

import java.io.InputStream;

/**
 * @author Renze de Vries
 */
public class DataKey extends AbstractKey {
    private ClonableDataStream stream;

    public DataKey(byte[] data) {
        this.stream = new ClonableByteArrayInputStream(data);
    }

    public DataKey(ClonableDataStream stream) {
        this.stream = stream;
    }

    public ClonableDataStream getStream() throws JasDBStorageException {
        try {
            return stream.clone();
        } catch(CloneNotSupportedException e) {
            throw new JasDBStorageException("Unable to clone datastream", e);
        }
    }

    @Override
    public Key cloneKey() {
        return new DataKey(stream);
    }

    @Override
    public Key cloneKey(boolean includeChildren) {
        return cloneKey();
    }

    @Override
    public int compareTo(Key o) {
        throw new RuntimeJasDBException("Cannot use data key for compare or sorting");
    }

    @Override
    public CompareResult compare(Key otherKey, CompareMethod method) {
        return null;
    }

    @Override
    public InputStream getValue() {
        return stream;
    }
}
