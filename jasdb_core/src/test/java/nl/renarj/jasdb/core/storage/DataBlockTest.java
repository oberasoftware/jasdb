package nl.renarj.jasdb.core.storage;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockFactory;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockHeader;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockResult;
import nl.renarj.jasdb.core.storage.datablocks.impl.BlockDataInputStream;
import nl.renarj.jasdb.core.storage.datablocks.impl.DataBlockFactoryImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public class DataBlockTest {
    private static final Logger LOG = LoggerFactory.getLogger(DataBlockTest.class);

    private static final int BLOCK_SIZE = 8192;
    private static final String IMAGE_RESOURCE = "/data/P1000003.png";

    private static final String UTF_ENCODING = "UTF-8";
    private static final String TEST_STRING1 = "Some text String XXXXX";
    private static final String TEST_STRING2 = "Some text String 2";
    private static final String TEST_STRING3 = "Some other String text ending with YYYY";
    private static final String TEST_STRING4 = "Then we start with some more text ending with PPPP";


    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWriteAndLoadBlock() throws IOException, JasDBStorageException {
        File file = temporaryFolder.newFile();
        LOG.info("Writing to data file: {}", file.toString());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        DataBlockFactory blockFactory = new DataBlockFactoryImpl(file, channel, BLOCK_SIZE);
        try {
            DataBlock dataBlock = blockFactory.getBlockWithSpace(false);

            long dataString1Position = dataBlock.writeBytes(TEST_STRING1.getBytes(UTF_ENCODING)).getDataPosition();
            long dataString2Position = dataBlock.writeBytes(TEST_STRING2.getBytes(UTF_ENCODING)).getDataPosition();
            long dataString3Position = dataBlock.writeBytes(TEST_STRING3.getBytes(UTF_ENCODING)).getDataPosition();
            long dataString4Position = dataBlock.writeBytes(TEST_STRING4.getBytes(UTF_ENCODING)).getDataPosition();


            assertEquals(TEST_STRING1, new String(dataBlock.loadBytes(dataString1Position).getValue(), UTF_ENCODING));
            assertEquals(TEST_STRING2, new String(dataBlock.loadBytes(dataString2Position).getValue(), UTF_ENCODING));
            assertEquals(TEST_STRING3, new String(dataBlock.loadBytes(dataString3Position).getValue(), UTF_ENCODING));
            assertEquals(TEST_STRING4, new String(dataBlock.loadBytes(dataString4Position).getValue(), UTF_ENCODING));
        } finally {
            channel.close();
            raf.close();
        }
    }

    @Test
    public void testWriteStreamNextDataPosition() throws IOException, JasDBStorageException {
        File file = temporaryFolder.newFile();
        LOG.info("Writing to data file: {}", file.toString());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        DataBlockFactory blockFactory = new DataBlockFactoryImpl(file, channel, BLOCK_SIZE);
        try {
            DataBlock block = blockFactory.getBlockWithSpace(false);

            DataBlock.WriteResult result = writeResource(IMAGE_RESOURCE, block);
            long firstStream = result.getDataPosition();
            long endDataBlockPosition = result.getDataBlock().getPosition();
            result = writeResource(IMAGE_RESOURCE, result.getDataBlock());
            long secondStream = result.getDataPosition();
            assertTrue(firstStream != secondStream);

            DataBlockResult<BlockDataInputStream> loadResult = block.loadStream(firstStream);
            assertEquals(endDataBlockPosition, loadResult.getEndBlock().getPosition());
            assertTrue(secondStream < endDataBlockPosition + (BLOCK_SIZE - DataBlockHeader.HEADER_SIZE));
            assertEquals(secondStream % blockFactory.getBlockSize() - DataBlockHeader.HEADER_SIZE, loadResult.getNextOffset());
        } finally {
            channel.close();
            raf.close();
        }
    }

    @Test
    public void testNextOffset() throws IOException, JasDBStorageException {
        File file = temporaryFolder.newFile();
        LOG.info("Writing to data file: {}", file.toString());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        DataBlockFactory blockFactory = new DataBlockFactoryImpl(file, channel, BLOCK_SIZE);
        try {
            DataBlock dataBlock = blockFactory.getBlockWithSpace(false);
            long long1Position = dataBlock.writeLong(10l).getDataPosition();
            long long2Position = dataBlock.writeLong(20l).getDataPosition();
            long long3Position = dataBlock.writeLong(30l).getDataPosition();
            long long4Position = dataBlock.writeLong(40l).getDataPosition();

            assertEquals(16, dataBlock.loadLong(long1Position).getNextOffset());
            assertEquals(32, dataBlock.loadLong(long2Position).getNextOffset());
            assertEquals(48, dataBlock.loadLong(long3Position).getNextOffset());
            assertEquals(64, dataBlock.loadLong(long4Position).getNextOffset());
        } finally {
            channel.close();
            raf.close();
        }
    }

    @Test
    public void testNextOffsetEndBlock() throws IOException, JasDBStorageException {
        File file = temporaryFolder.newFile();
        LOG.info("Writing to data file: {}", file.toString());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        DataBlockFactory blockFactory = new DataBlockFactoryImpl(file, channel, BLOCK_SIZE);
        try {
            DataBlock dataBlock = blockFactory.getBlockWithSpace(false);
            DataBlock.WriteResult writeResult = dataBlock.writeBytes(new byte[7999]);

            DataBlockResult<byte[]> loadResult = dataBlock.loadBytes(writeResult.getDataPosition());
            assertEquals(8007, loadResult.getNextOffset());
        } finally {
            channel.close();
            raf.close();
        }
    }

    @Test
    public void testWriteReadLong() {

    }

    /**
     * Special case to test if we reach end of block and not enough space for
     * writing the data stream header.
     */
    @Test
    public void testLoadEndOfBlockNewDatastream() {

    }

    @Test
    public void testOverwriteBlockData() {

    }

    @Test
    public void testOverwriteBlockChain() {

    }

    @Test
    public void testWriteAndLoadBinaryData() throws IOException, JasDBStorageException {
        File file = temporaryFolder.newFile();
        LOG.info("Writing to data file: {}", file.toString());
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel channel = raf.getChannel();

        DataBlockFactory blockFactory = new DataBlockFactoryImpl(file, channel, BLOCK_SIZE);
        try {
            DataBlock block = blockFactory.getBlockWithSpace(false);

            InputStream stream = this.getClass().getResourceAsStream(IMAGE_RESOURCE);
            CheckedInputStream checkedInputStream = new CheckedInputStream(stream, new CRC32());
            DataBlock.WriteResult writeResult = block.writeStream(checkedInputStream);
            long crcValue = checkedInputStream.getChecksum().getValue();

            LOG.info("Finished writing stream, crc: {}", crcValue);
            LOG.info("Nr of loaded blocks: {}", blockFactory.getCachedBlocks());
            LOG.info("Bytes written: {}", writeResult.bytesWritten());

            InputStream readStream = block.loadStream(writeResult.getDataPosition()).getValue();
            checkedInputStream = new CheckedInputStream(readStream, new CRC32());
            byte[] buffer = new byte[4096];

            while((checkedInputStream.read(buffer)) != -1) {
            }

            assertEquals("Expected equal crc", crcValue, checkedInputStream.getChecksum().getValue());
        } finally {
            channel.close();
            raf.close();
        }
    }

    private DataBlock.WriteResult writeResource(String resource, DataBlock dataBlock) throws JasDBStorageException {
        InputStream stream = this.getClass().getResourceAsStream(resource);
        CheckedInputStream checkedInputStream = new CheckedInputStream(stream, new CRC32());
        DataBlock.WriteResult writeResult = dataBlock.writeStream(checkedInputStream);

        return writeResult;
    }
}
