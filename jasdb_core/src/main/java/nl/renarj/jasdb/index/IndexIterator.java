package nl.renarj.jasdb.index;

import nl.renarj.jasdb.index.keys.Key;

import java.util.Iterator;

/**
 * @author Renze de Vries
 */
public interface IndexIterator extends Iterator<Key>, Iterable<Key> {
    void close();

    void reset();
}
