package com.oberasoftware.jasdb.api.storage;

import com.oberasoftware.jasdb.api.concurrency.ReadWriteLock;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author Renze de Vries
 */
public interface DataBlock extends Block {
    DataBlockHeader getHeader();

    DataBlock loadNext() throws JasDBStorageException;

    void flush() throws JasDBStorageException;

    /**
     * Returns the data size of the block on the disk including the size of the
     * header.
     * @return The size of the data block on the disk including header
     */
    int size();

    /**
     * The capacity of the block available for storage, this is excluding the
     * size of the header.
     * @return The storage capacity of the block
     */
    int capacity();

    int available();

    void reset();

    ByteBuffer getBuffer();

    ReadWriteLock getLockManager();

    /**
     * Load bytes from the datablock, providing an offset from the start of the block.
     * If the offset exceeds the block size it will start loading from the block chain.
     *
     * @param offset The offset to start loading the data from
     * @return The bytes loaded fully into memory
     * @throws JasDBStorageException If unable to load the bytes from the block
     */
    DataBlockResult<byte[]> loadBytes(int offset) throws JasDBStorageException;

    DataBlockResult<byte[]> loadBytes(long absolutePosition) throws JasDBStorageException;

    DataBlockResult<ClonableDataStream> loadStream(int offset) throws JasDBStorageException;

    DataBlockResult<ClonableDataStream> loadStream(long absolutePosition) throws JasDBStorageException;

    DataBlockResult<Long> loadLong(int offset) throws JasDBStorageException;

    DataBlockResult<Long> loadLong(long absolutePosition) throws JasDBStorageException;

    WriteResult writeBytes(byte[] bytes) throws JasDBStorageException;

    WriteResult writeStream(InputStream stream) throws JasDBStorageException;

    WriteResult writeLong(long value) throws JasDBStorageException;

    public interface WriteResult {
        long bytesWritten();

        long getDataPosition();

        DataBlock getDataBlock();
    }
}
