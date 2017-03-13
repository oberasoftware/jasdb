package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.keys.Key;

import java.util.List;

/**
 * @author Renze de Vries
 */
public interface LeaveBlock extends IndexBlock {
    void insertKey(Key key) throws JasDBStorageException;

    void updateKey(Key key) throws JasDBStorageException;

    void removeKey(Key key) throws JasDBStorageException;

    boolean contains(Key key);

    int size();

    Key getKey(Key key);

    List<Key> getKeyRange(Key start, boolean includeStart, Key end, boolean includeEnd);

    List<Key> getValues();

    LeaveBlockProperties getProperties();
}
