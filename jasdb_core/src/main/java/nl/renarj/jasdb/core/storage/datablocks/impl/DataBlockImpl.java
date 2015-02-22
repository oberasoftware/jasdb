package nl.renarj.jasdb.core.storage.datablocks.impl;

import nl.renarj.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockFactory;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockHeader;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockResult;
import nl.renarj.jasdb.core.utils.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

/**
 * @author Renze de Vries
 */
public class DataBlockImpl implements DataBlock {
    private static final Logger LOG = LoggerFactory.getLogger(DataBlockImpl.class);

    public static final int STREAM_HEADER_SPACE = MEMORY_CONSTANTS.LONG_BYTE_SIZE;
    private static final int BUFFER_SIZE = 4096;

    private MappedByteBuffer mappedByteBuffer;
    private DataBlockFactory dataBlockFactory;

    private long position;
    private DataBlockHeader header;
    private ReadWriteLock lockManager = new ReadWriteLock();

    public DataBlockImpl(long position, DataBlockFactory dataBlockFactory, MappedByteBuffer mappedByteBuffer) {
        this.position = position;

        this.mappedByteBuffer = mappedByteBuffer;
        this.dataBlockFactory = dataBlockFactory;
        header = new DataBlockHeaderImpl(mappedByteBuffer);
    }

    @Override
    public DataBlock loadNext() throws JasDBStorageException {
        return dataBlockFactory.loadBlock(header.getNext());
    }

    @Override
    public long getPosition() {
        return position;
    }

    @Override
    public DataBlockHeader getHeader() {
        return header;
    }

    @Override
    public void reset() {
        header.resetMarker();
    }

    @Override
    public int available() {
        return (dataBlockFactory.getBlockSize() - (header.marker() + DataBlockHeader.HEADER_SIZE));
    }

    @Override
    public DataBlockResult<byte[]> loadBytes(int offset) throws JasDBStorageException {
        DataBlockResult<BlockDataInputStream> result = loadStream(offset);
        if(result.getDataLength() < Integer.MAX_VALUE) {
            ByteArrayOutputStream out = new ByteArrayOutputStream((int) result.getDataLength());
            try {
                try (BlockDataInputStream inputStream = result.getValue()) {
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int read;
                    while ((read = inputStream.read(buffer)) != -1) {
                        out.write(buffer, 0, read);
                    }
                }
            } catch(IOException e) {
                throw new JasDBStorageException("Unable to load byte stream", e);
            }

            return new DataBlockResult<>(result.getDataLength(), result.getEndBlock(), result.getNextOffset(), out.toByteArray());
        } else {
            throw new JasDBStorageException("Unable to load into byte array exceeding maximum byte array length of Max Integer");
        }
    }

    @Override
    public DataBlockResult<byte[]> loadBytes(long absolutePosition) throws JasDBStorageException {
        LOG.debug("Loading at absolute position: {}", absolutePosition);
        int relativeOffset = getRelativePosition(absolutePosition);

        return loadBytes(relativeOffset);
    }

    @Override
    public DataBlockResult<BlockDataInputStream> loadStream(long absolutePosition) throws JasDBStorageException {
        LOG.debug("Loading at absolute position: {}", absolutePosition);
        int relativeOffset = getRelativePosition(absolutePosition);

        return loadStream(relativeOffset);
    }

    private int getRelativePosition(long absolutePosition) throws JasDBStorageException {
        if(absolutePosition > position && (absolutePosition < getPosition() + dataBlockFactory.getBlockSize())) {
            return (int) (absolutePosition % dataBlockFactory.getBlockSize()) - DataBlockHeader.HEADER_SIZE;
        } else {
            throw new JasDBStorageException("Unable to load bytes from this block, absolute position falls outside block");
        }
    }

    @Override
    public MappedByteBuffer getBuffer() {
        return mappedByteBuffer;
    }

    @Override
    public ReadWriteLock getLockManager() {
        return lockManager;
    }

    @Override
    public int capacity() {
        return dataBlockFactory.getBlockSize() - DataBlockHeader.HEADER_SIZE;
    }

    @Override
    public int size() {
        return dataBlockFactory.getBlockSize();
    }

    @Override
    public DataBlockResult<BlockDataInputStream> loadStream(final int offset) throws JasDBStorageException {
        int offSetWithHeader = offset + DataBlockHeader.HEADER_SIZE;
        int available = dataBlockFactory.getBlockSize() - offSetWithHeader;

        if(available < STREAM_HEADER_SPACE) {
            byte[] valueBytes = new byte[MEMORY_CONSTANTS.LONG_BYTE_SIZE];
            mappedByteBuffer.position(offSetWithHeader);
            mappedByteBuffer.get(valueBytes, 0, available);
            int remaining = MEMORY_CONSTANTS.LONG_BYTE_SIZE - available;

            DataBlockImpl nextBlock = (DataBlockImpl) dataBlockFactory.loadBlock(header.getNext());
            ByteBuffer nextBuffer = nextBlock.getBuffer();
            nextBuffer.position(DataBlockHeader.HEADER_SIZE);
            nextBuffer.get(valueBytes, available, remaining);

            long dataLength = convertLong(valueBytes);

            return loadBlockStream(nextBlock, remaining + DataBlockHeader.HEADER_SIZE, dataLength);
        } else {
            final long dataLength = mappedByteBuffer.getLong(offSetWithHeader);

            return loadBlockStream(this, offSetWithHeader + STREAM_HEADER_SPACE, dataLength);
        }
    }

    private DataBlockResult<BlockDataInputStream> loadBlockStream(DataBlockImpl initialBlock, int offset, long dataLength) throws JasDBStorageException {
        BlockDataInputStream inputStream = new BlockDataInputStream(initialBlock, offset, dataLength, dataBlockFactory);
        if(((offset + dataLength) - DataBlockHeader.HEADER_SIZE) > capacity()) {
            //data will cross block boundry
            long nextDataStream = initialBlock.getHeader().getNextStream();
            int dataStreamOffset = ((int) (nextDataStream % dataBlockFactory.getBlockSize()) - DataBlockHeader.HEADER_SIZE);

            return new DataBlockResult<>(dataLength, dataBlockFactory.loadBlockForDataPosition(nextDataStream), dataStreamOffset, inputStream);
        } else {
            //inside same block
            return new DataBlockResult<>(dataLength, initialBlock, ((int) (offset + dataLength)) - DataBlockHeader.HEADER_SIZE, inputStream);
        }
    }

    @Override
    public DataBlockResult<Long> loadLong(int offset) throws JasDBStorageException {
        DataBlockResult<byte[]> result = loadBytes(offset);
        return new DataBlockResult<>(result.getDataLength(), result.getEndBlock(), result.getNextOffset(), convertLong(result.getValue()));
    }

    @Override
    public DataBlockResult<Long> loadLong(long absolutePosition) throws JasDBStorageException {
        int relativeOffset = getRelativePosition(absolutePosition);

        return loadLong(relativeOffset);
    }

    @Override
    public WriteResult writeBytes(byte[] bytes) throws JasDBStorageException {
        if(bytes.length > dataBlockFactory.getBlockSize()) {
            throw new JasDBStorageException("Unable to store raw bytes larger than block size");
        } else {
            return writeStream(new ByteArrayInputStream(bytes));
        }
    }

    @Override
    public WriteResult writeStream(InputStream stream) throws JasDBStorageException {
        return writeDataStream(stream, false);
    }

    public WriteResult writeDataStream(InputStream stream, boolean isDelegate) throws JasDBStorageException {
        LOG.debug("Writing data stream: {}, delegate: {}", stream, isDelegate);
        if(available() > STREAM_HEADER_SPACE) {
            DataBlockImpl currentBlock = this;
            byte[] buffer = new byte[BUFFER_SIZE];
            try {
                int blockSize = dataBlockFactory.getBlockSize();
                long bytesWritten = 0;
                long dataPosition = position + DataBlockHeader.HEADER_SIZE + header.marker();

                if(!isDelegate) {
                    header.incrementMarker(STREAM_HEADER_SPACE);
                }

                int read;
                while((read = stream.read(buffer)) > -1) {
                    currentBlock.getLockManager().writeLock();
                    try {
                        MappedByteBuffer byteBuffer = currentBlock.getBuffer();
                        byteBuffer.position(currentBlock.getHeader().markerWithHeader());

                        bytesWritten += read;
                        int spaceLeft = blockSize - currentBlock.getHeader().markerWithHeader();
                        if(spaceLeft < read) {
                            LOG.debug("Having overflow on block: {}", currentBlock);
                            byte[] partial = Arrays.copyOf(buffer, spaceLeft);
                            byteBuffer.put(partial);
                            currentBlock.getHeader().incrementMarker(spaceLeft);

                            DataBlockImpl dataBlock = allocateBlock(currentBlock, true);
                            int overflow = read - spaceLeft;
                            dataBlock.getBuffer().position(DataBlockHeader.HEADER_SIZE);
                            dataBlock.getBuffer().put(Arrays.copyOfRange(buffer, spaceLeft, read));
                            dataBlock.getHeader().incrementMarker(overflow);

                            currentBlock.getLockManager().writeUnlock();
                            currentBlock = dataBlock;
                        } else {
                            LOG.debug("Doing normal write: {}", currentBlock);
                            byteBuffer.put(buffer, 0, read);
                            currentBlock.getHeader().incrementMarker(read);
                        }
                    } finally {
                        currentBlock.getLockManager().writeUnlock();
                    }
                }
                if(!isDelegate) {
                    mappedByteBuffer.putLong((int) (dataPosition - position), bytesWritten);
                    header.setNextStream(currentBlock.getPosition() + DataBlockHeader.HEADER_SIZE + currentBlock.getHeader().marker());
                }

                return new WriteResultImpl(dataPosition, bytesWritten, currentBlock);
            } catch(IOException e) {
                throw new JasDBStorageException("Unable to write data stream", e);
            }
        } else {
            int advanceNextBlockMarker = STREAM_HEADER_SPACE - available();
            DataBlockImpl dataBlock = allocateBlock(this, false);
            dataBlock.getHeader().incrementMarker(advanceNextBlockMarker);

            WriteResult result = dataBlock.writeDataStream(stream, true);
            long bytesWritten = result.bytesWritten();
            byte[] byteValue = convertLong(bytesWritten);
            mappedByteBuffer.put(Arrays.copyOf(byteValue, available()));

            MappedByteBuffer buffer = dataBlock.getBuffer();
            buffer.position(DataBlockHeader.HEADER_SIZE);
            buffer.put(Arrays.copyOfRange(byteValue, available(), MEMORY_CONSTANTS.LONG_BYTE_SIZE));
            header.setNextStream(result.getDataBlock().getPosition() + DataBlockHeader.HEADER_SIZE + result.getDataBlock().getHeader().marker());

            return result;
        }
    }

    private DataBlockImpl allocateBlock(DataBlock previous, boolean lock) throws JasDBStorageException {
        if(previous.getHeader().getNext() > 0) {
            DataBlockImpl dataBlock = (DataBlockImpl) previous.loadNext();
            dataBlock.reset();

            if(lock) {
                dataBlock.getLockManager().writeLock();
            }

            return dataBlock;
        } else {
            DataBlockImpl dataBlock = (DataBlockImpl) dataBlockFactory.getBlockWithSpace(false);
            dataBlock.getLockManager().writeLock();
            try {
                dataBlock.getHeader().setPrevious(previous.getPosition());
                previous.getHeader().setNext(dataBlock.getPosition());
            } finally {
                if(!lock) {
                    dataBlock.getLockManager().writeUnlock();
                }
            }

            return dataBlock;
        }
    }

    private byte[] convertLong(long value) {
        return ByteBuffer.allocate(MEMORY_CONSTANTS.LONG_BYTE_SIZE).putLong(value).array();
    }

    private long convertLong(byte[] value) {
        return ByteBuffer.wrap(value).getLong(0);
    }

    @Override
    public WriteResult writeLong(long value) throws JasDBStorageException {
        return writeBytes(convertLong(value));
    }

    @Override
    public void close() throws JasDBStorageException {
        flush();
    }

    @Override
    public void flush() throws JasDBStorageException {
        mappedByteBuffer.force();
    }

    private class WriteResultImpl implements WriteResult {
        private long dataPosition;
        private long bytesWritten;
        private DataBlock dataBlock;

        public WriteResultImpl(long dataPosition, long bytesWritten, DataBlock dataBlock) {
            this.dataPosition = dataPosition;
            this.bytesWritten = bytesWritten;
            this.dataBlock = dataBlock;
        }

        @Override
        public DataBlock getDataBlock() {
            return dataBlock;
        }

        @Override
        public long bytesWritten() {
            return bytesWritten;
        }

        @Override
        public long getDataPosition() {
            return dataPosition;
        }
    }

    @Override
    public String toString() {
        return "DataBlockImpl{" +
                "position=" + position +
                ", header=" + header +
                '}';
    }
}
