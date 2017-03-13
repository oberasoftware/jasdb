package com.oberasoftware.jasdb.api.index;

import com.oberasoftware.jasdb.api.index.keys.Key;

import java.util.Iterator;

/**
 * @author Renze de Vries
 */
public interface IndexIterator extends Iterator<Key>, Iterable<Key> {
    void close();

    void reset();
}
