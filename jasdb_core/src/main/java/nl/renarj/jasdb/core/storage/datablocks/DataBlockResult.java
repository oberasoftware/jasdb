package nl.renarj.jasdb.core.storage.datablocks;

/**
 * Represents the data result loaded from the block, containing information
 * about data offset, end block and value
 *
 * @author Renze de Vries
 */
public class DataBlockResult<T> {
    private long dataLength;
    private DataBlock endBlock;
    private int nextOffset;
    private T value;

    public DataBlockResult(long dataLength, DataBlock endBlock, int nextOffset, T value) {
        this.nextOffset = nextOffset;
        this.dataLength = dataLength;
        this.endBlock = endBlock;
        this.value = value;
    }

    /**
     * The next data stream offset in the end block
     * @return The next data stream offset
     */
    public int getNextOffset() {
        return nextOffset;
    }

    /**
     * The length of the data
     * @return The length of the data
     */
    public long getDataLength() {
        return dataLength;
    }

    /**
     * The end block containing the tail of the data
     * @return The end block
     */
    public DataBlock getEndBlock() {
        return endBlock;
    }

    /**
     * The loaded data
     * @return The loaded data
     */
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
