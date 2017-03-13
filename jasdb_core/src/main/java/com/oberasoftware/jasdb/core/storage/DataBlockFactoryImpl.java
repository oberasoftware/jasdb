package com.oberasoftware.jasdb.core.storage;

import com.oberasoftware.jasdb.core.caching.LRURegion;
import com.oberasoftware.jasdb.api.storage.Block;
import com.oberasoftware.jasdb.api.storage.DataBlock;
import com.oberasoftware.jasdb.api.storage.DataBlockFactory;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Renze de Vries
 */
public class DataBlockFactoryImpl implements DataBlockFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DataBlockFactoryImpl.class);

    private static final int HEADER_POSITION = 0;

    private int blockSize;
    private long initialPosition = 0;

    private LRURegion<BlockEntry<DataBlock>> activeBlockRegion;
    private AtomicLong nextPosition = new AtomicLong(initialPosition);

    private FileChannel fileChannel;

    public DataBlockFactoryImpl(File file, FileChannel fileChannel, int blockSize) {
        this.blockSize = blockSize;
        this.fileChannel = fileChannel;

        activeBlockRegion = new LRURegion<>(file.toString());
//        GlobalCachingMemoryManager.getGlobalInstance().registerRegion(activeBlockRegion);
    }

    @Override
    public void open() throws JasDBStorageException {
        try {
            //make sure we always allow the header block first, which is one data block
            long initialPosition = fileChannel.size() > 0 ? fileChannel.size() : blockSize;
            this.nextPosition = new AtomicLong(initialPosition);
        } catch(IOException e) {
            throw new JasDBStorageException("Unable to open file channel for data block storage", e);
        }
    }

    @Override
    public DataBlock getHeaderBlock() throws JasDBStorageException {
        return loadBlock(HEADER_POSITION);
    }

    @Override
    public int getBlockSize() {
        return this.blockSize;
    }

    @Override
    public long getTotalMemoryUsage() {
        return blockSize * activeBlockRegion.size();
    }

    @Override
    public long getCachedBlocks() {
        return activeBlockRegion.size();
    }

    @Override
    public DataBlock loadBlock(long position) throws JasDBStorageException {
        BlockEntry<DataBlock> blockEntry = activeBlockRegion.getEntry(position);
        if(blockEntry != null) {
            blockEntry.incrementBlockCount();

            return blockEntry.getValue();
        } else {
            LOG.debug("Loading block at position: {}", position);
            try {
                MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, position, blockSize);

                DataBlock dataBlock = new DataBlockImpl(position, this, mappedByteBuffer);
                BlockEntry<DataBlock> entry = new BlockEntry<>(dataBlock);
                entry.incrementBlockCount();
                activeBlockRegion.putEntry(position, entry);

                return dataBlock;
            } catch(IOException e) {
                throw new JasDBStorageException("Unable to map filechannel to memory", e);
            }
        }
    }

    @Override
    public DataBlock loadBlockForDataPosition(long dataPosition) throws JasDBStorageException {
        long remaining = dataPosition % blockSize;
        long blockPosition = dataPosition - remaining;

        return loadBlock(blockPosition);
    }

    @Override
    public DataBlock getBlockWithSpace(boolean allowFragmented) throws JasDBStorageException {
        try {
            long recordPosition = nextPosition.getAndAdd(blockSize);
            LOG.debug("Creating block at position: {}", recordPosition);

            DataBlock block = new DataBlockImpl(recordPosition, this, fileChannel.map(FileChannel.MapMode.READ_WRITE, recordPosition, blockSize));

            BlockEntry<DataBlock> entry = new BlockEntry<>(block);
            entry.incrementBlockCount();
            activeBlockRegion.putEntry(block.getPosition(), entry);

            return block;
        } catch(IOException e) {
            throw new JasDBStorageException("Unable to create new data block", e);
        }
    }

    @Override
    public void releaseBlock(long position) throws JasDBStorageException {
        activeBlockRegion.getEntry(position).decrementBlockCount();
    }

    @Override
    public void flush() throws JasDBStorageException {
        for(BlockEntry entry : activeBlockRegion.values()) {
            Block block = entry.getValue();
            block.close();
        }
    }

    @Override
    public void close() throws JasDBStorageException {
        flush();
        activeBlockRegion.clear();
    }
}
