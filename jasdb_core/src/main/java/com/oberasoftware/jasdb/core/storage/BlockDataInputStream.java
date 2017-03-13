package com.oberasoftware.jasdb.core.storage;

import com.oberasoftware.jasdb.api.storage.DataBlockHeader;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.DataBlockFactory;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author Renze de Vries
 */
public class BlockDataInputStream extends ClonableDataStream {
    private static final Logger LOG = LoggerFactory.getLogger(BlockDataInputStream.class);

    private final int blockSize;
    private int blockMark;
    private long dataMark = 0;
    private final long dataLength;
    private DataBlockImpl currentBlock;
    private DataBlockFactory dataBlockFactory;

    private DataBlockImpl startBlock;
    private int initialOffset;

    public BlockDataInputStream(DataBlockImpl startBlock, final int offset, final long dataLength, DataBlockFactory dataBlockFactory) {
        this.dataBlockFactory = dataBlockFactory;
        this.dataLength = dataLength;
        this.blockMark = offset;
        this.currentBlock = startBlock;
        this.blockSize = dataBlockFactory.getBlockSize();
        this.startBlock = startBlock;
        this.initialOffset = offset;
    }

    @Override
    public BlockDataInputStream clone() throws CloneNotSupportedException {
        return new BlockDataInputStream(startBlock, initialOffset, dataLength, dataBlockFactory);
    }

    @Override
    public int read() throws IOException {
        byte[] buffer = new byte[1];
        if(read(buffer, 0, 1) == 1) {
            return ((Byte)buffer[0]).intValue() & 0xff;
        }
        return -1;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int bufferOffset, int readLength) throws IOException {
        try {
            LOG.debug("Reading into byte array of length: {}", readLength);
            if(dataMark < dataLength) {
                long remainingData = dataLength - dataMark;
                int len = remainingData > readLength ? readLength : (int)remainingData;
                int readAmount = 0;

                while(dataMark < dataLength && readAmount < readLength) {
                    try {
                        LOG.debug("Reading from block: {} marker: {}", currentBlock, blockMark);
                        LOG.debug("Data mark: {} data length: {}", dataMark, dataLength);
                        currentBlock.getLockManager().readLock();
                        int blockRemaining = blockSize - blockMark;
                        ByteBuffer buffer = currentBlock.getBuffer().duplicate();

                        buffer.position(blockMark);

                        if(len > blockRemaining) {
                            LOG.debug("Block end read: {} bytes into buffer offset: {}", blockRemaining, bufferOffset);

                            buffer.get(b, bufferOffset, blockRemaining);
                            blockMark = DataBlockHeader.HEADER_SIZE;
                            currentBlock.getLockManager().readUnlock();
                            currentBlock = (DataBlockImpl) dataBlockFactory.loadBlock(currentBlock.getHeader().getNext());
                            currentBlock.getLockManager().readLock();
                            bufferOffset += blockRemaining;
                            dataMark += blockRemaining;
                            readAmount += blockRemaining;
                            len = len - blockRemaining;
                        } else {
                            LOG.debug("Normal read: {} bytes into buffer offset: {}", len, bufferOffset);

                            buffer.get(b, bufferOffset, len);
                            bufferOffset = 0;
                            blockMark += len;
                            dataMark += len;
                            readAmount += len;
                        }
                    } finally {
                        currentBlock.getLockManager().readUnlock();
                    }
                }

                return readAmount;
            } else {
                return -1;
            }
        } catch(JasDBStorageException e) {
            throw new IOException("", e);
        }
    }


    @Override
    public synchronized void reset() throws IOException {

    }
}
