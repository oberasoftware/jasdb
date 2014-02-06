package nl.renarj.jasdb.core.storage.datablocks;

import nl.renarj.jasdb.core.caching.MemoryAware;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;

/**
 * @author Renze de Vries
 */
public interface DataBlockFactory extends MemoryAware {
    void open() throws JasDBStorageException;

    int getBlockSize();

    DataBlock getHeaderBlock() throws JasDBStorageException;

    DataBlock loadBlock(long position) throws JasDBStorageException;

    DataBlock loadBlockForDataPosition(long dataPosition) throws JasDBStorageException;

    DataBlock getBlockWithSpace(boolean allowFragmented) throws JasDBStorageException;

    void releaseBlock(long position) throws JasDBStorageException;

    void flush() throws JasDBStorageException;

    void close() throws JasDBStorageException;
}
