package nl.renarj.jasdb.core.streams;

import java.io.InputStream;

/**
 * @author Renze de Vries
 */
public abstract class ClonableDataStream extends InputStream {
    /**
     * This creates a cloned data stream that allows multiple threads to read
     * the resource in question.
     * @return A cloned reusable data stream
     */
    public abstract ClonableDataStream clone() throws CloneNotSupportedException;
}
