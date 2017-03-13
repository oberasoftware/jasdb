package com.oberasoftware.jasdb.core.storage;

import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockResult;

/**
 * Represents the data result loaded from the block, containing information
 * about data offset, end block and value
 *
 * @author Renze de Vries
 */
public class DataBlockResultImpl<T> implements DataBlockResult<T> {
    private long dataLength;
    private DataBlock endBlock;
    private int nextOffset;
    private T value;

    public DataBlockResultImpl(long dataLength, DataBlock endBlock, int nextOffset, T value) {
        this.nextOffset = nextOffset;
        this.dataLength = dataLength;
        this.endBlock = endBlock;
        this.value = value;
    }

    /**
     * The next data stream offset in the end block
     * @return The next data stream offset
     */
    @Override
    public int getNextOffset() {
        return nextOffset;
    }

    /**
     * The length of the data
     * @return The length of the data
     */
    @Override
    public long getDataLength() {
        return dataLength;
    }

    /**
     * The end block containing the tail of the data
     * @return The end block
     */
    @Override
    public DataBlock getEndBlock() {
        return endBlock;
    }

    /**
     * The loaded data
     * @return The loaded data
     */
    @Override
    public T getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "DataBlockResult{" +
                "dataLength=" + dataLength +
                ", endBlock=" + endBlock +
                ", nextOffset=" + nextOffset +
                ", value=" + value +
                '}';
    }
}
