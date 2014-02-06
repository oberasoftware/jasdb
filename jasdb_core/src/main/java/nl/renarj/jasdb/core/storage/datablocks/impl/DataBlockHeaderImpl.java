package nl.renarj.jasdb.core.storage.datablocks.impl;

import nl.renarj.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockHeader;

import java.nio.ByteBuffer;

/**
 * @author Renze de Vries
 */
public class DataBlockHeaderImpl implements DataBlockHeader {
    /**
     * Next and previous + marker = 28 bytes for mandatory header in datablock
     */
    private static final int BLOCK_MANDATORY_HEADER = MEMORY_CONSTANTS.TWO_LONG_BYTES + MEMORY_CONSTANTS.LONG_BYTE_SIZE + MEMORY_CONSTANTS.INTEGER_BYTE_SIZE;

    public static final int PREVIOUS_INDEX = 0; //start of the block
    public static final int NEXT_INDEX = MEMORY_CONSTANTS.LONG_BYTE_SIZE; //after the 'previous' long
    public static final int NEXTSTREAM_INDEX = MEMORY_CONSTANTS.TWO_LONG_BYTES; //location of next datastream (optional)
    public static final int MARKER_INDEX = MEMORY_CONSTANTS.TWO_LONG_BYTES + MEMORY_CONSTANTS.LONG_BYTE_SIZE; //after the 'previous', 'next' and 'next stream' longs


    private ByteBuffer byteBuffer;

    public DataBlockHeaderImpl(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public long getNext() {
        return byteBuffer.getLong(NEXT_INDEX);
    }

    @Override
    public long getPrevious() {
        return byteBuffer.getLong(PREVIOUS_INDEX);
    }

    @Override
    public void setNext(long next) {
        byteBuffer.putLong(NEXT_INDEX, next);
    }

    @Override
    public void setPrevious(long previous) {
        byteBuffer.putLong(PREVIOUS_INDEX, previous);
    }

    @Override
    public int marker() {
        return byteBuffer.getInt(MARKER_INDEX);
    }

    @Override
    public int markerWithHeader() {
        return marker() + DataBlockHeader.HEADER_SIZE;
    }

    @Override
    public void setNextStream(long nextStreamLocation) {
        this.byteBuffer.putLong(NEXTSTREAM_INDEX, nextStreamLocation);
    }

    @Override
    public long getNextStream() {
        return this.byteBuffer.getLong(NEXTSTREAM_INDEX);
    }

    @Override
    public void incrementMarker(int amount) {
        int newMarker = marker() + amount;
        byteBuffer.putInt(MARKER_INDEX, newMarker);
    }

    @Override
    public void resetMarker() {
        byteBuffer.putInt(MARKER_INDEX, 0);
    }

    @Override
    public void putLong(int offset, long value) throws JasDBStorageException {
        offsetCheck(offset);
        byteBuffer.putLong(offset + BLOCK_MANDATORY_HEADER, value);
    }

    @Override
    public void putInt(int offset, int value) throws JasDBStorageException {
        offsetCheck(offset);
        byteBuffer.putInt(offset + BLOCK_MANDATORY_HEADER, value);
    }

    private void offsetCheck(int offset) throws JasDBStorageException {
        if(offset >= (DataBlockHeader.HEADER_SIZE -  BLOCK_MANDATORY_HEADER)) {
            throw new JasDBStorageException("Illegal offset for header, offset needs smaller than block header size: "
                    + (DataBlockHeader.HEADER_SIZE - BLOCK_MANDATORY_HEADER));
        }
    }

    @Override
    public long getLong(int offset) throws JasDBStorageException {
        offsetCheck(offset);
        return byteBuffer.getLong(offset + BLOCK_MANDATORY_HEADER);
    }

    @Override
    public int getInt(int offset) throws JasDBStorageException {
        offsetCheck(offset);
        return byteBuffer.getInt(offset + BLOCK_MANDATORY_HEADER);
    }

    @Override
    public String toString() {
        return "DataBlockHeader{" +
                "next=" + getNext() +
                ", previous=" + getPrevious() +
                ", marker=" + marker() +
                '}';
    }

}
