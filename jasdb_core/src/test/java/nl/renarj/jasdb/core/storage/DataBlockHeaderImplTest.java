package nl.renarj.jasdb.core.storage;

import nl.renarj.jasdb.core.MEMORY_CONSTANTS;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockHeader;
import nl.renarj.jasdb.core.storage.datablocks.impl.DataBlockHeaderImpl;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/**
 * @author Renze de Vries
 */
public class DataBlockHeaderImplTest {
    @Test
    public void testHeaderMarker() {
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockHeader.HEADER_SIZE);
        DataBlockHeader header = new DataBlockHeaderImpl(buffer);

        assertEquals(0, header.marker());

        header.incrementMarker(400);
        assertEquals(400, header.marker());

        header.resetMarker();
        assertEquals(0, header.marker());
    }

    @Test
    public void testAll() throws JasDBStorageException {
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockHeader.HEADER_SIZE);
        DataBlockHeader header = new DataBlockHeaderImpl(buffer);

        assertEquals(0, header.getNext());
        assertEquals(0, header.getPrevious());
        assertEquals(0, header.marker());
        assertEquals(0, header.getNextStream());

        header.setNext(100l);
        header.setPrevious(200l);
        header.incrementMarker(500);
        header.setNextStream(600l);

        header.putLong(0, 700l);
        header.putLong(MEMORY_CONSTANTS.LONG_BYTE_SIZE, 800l);

        assertEquals(100l, header.getNext());
        assertEquals(200l, header.getPrevious());
        assertEquals(500, header.marker());
        assertEquals(600l, header.getNextStream());
        assertEquals(700l, header.getLong(0));
        assertEquals(800l, header.getLong(MEMORY_CONSTANTS.LONG_BYTE_SIZE));
    }

    @Test
    public void testHeaderNextStream() {
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockHeader.HEADER_SIZE);
        DataBlockHeader header = new DataBlockHeaderImpl(buffer);

        assertEquals(0, header.getNextStream());

        header.setNextStream(5000l);
        assertEquals(5000l, header.getNextStream());

        header.setNextStream(1l);
        assertEquals(1l, header.getNextStream());
    }

    @Test
    public void testHeaderNext() {
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockHeader.HEADER_SIZE);
        DataBlockHeader header = new DataBlockHeaderImpl(buffer);

        assertEquals(0, header.getNext());
        assertEquals(0, header.getPrevious());

        header.setNext(9999999992222l);
        assertEquals(9999999992222l, header.getNext());
        assertEquals(0, header.getPrevious());

        header.setNext(0);
        assertEquals(0, header.getNext());
        assertEquals(0, header.getPrevious());
    }

    @Test
    public void testHeaderPrevious() {
        ByteBuffer buffer = ByteBuffer.allocate(DataBlockHeader.HEADER_SIZE);
        DataBlockHeader header = new DataBlockHeaderImpl(buffer);

        assertEquals(0, header.getPrevious());
        assertEquals(0, header.getNext());

        header.setPrevious(9999999992222l);
        assertEquals(9999999992222l, header.getPrevious());
        assertEquals(0, header.getNext());

        header.setPrevious(0);
        assertEquals(0, header.getPrevious());
        assertEquals(0, header.getNext());
    }
}
