package nl.renarj.jasdb.storage;

import nl.renarj.core.exceptions.FileException;
import nl.renarj.core.utilities.ResourceUtil;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.RecordIterator;
import nl.renarj.jasdb.core.storage.RecordResult;
import nl.renarj.jasdb.core.storage.RecordWriter;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.storage.exceptions.RecordStoreInUseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Renze de Vries
 */
public abstract class BaseRecordWriterTest extends BaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(BaseRecordWriterTest.class);

    protected abstract RecordWriter createRecordWriter(File recordFile) throws JasDBStorageException;

    private static final String SOME_RECORD_CONTENTS = "Some Record Contents: ";

    @Before
    public void setUp() {
        clean();
    }

    @After
    public void tearDown() {
        clean();
    }

    private void clean() {
        assertDelete(new File(tmpDir, "bigstore.pjs"));
        assertDelete(new File(tmpDir, "bigstore.idx"));
        assertDelete(new File(tmpDir, "teststore.pjs"));
        assertDelete(new File(tmpDir, "teststore.idx"));
    }

    @Test(expected = RecordStoreInUseException.class)
    public void testWriterInUse() throws JasDBStorageException {
        File recordFile = new File(BaseTest.tmpDir, "teststore.pjs");
        RecordWriter recordWriter = createRecordWriter(recordFile);
        RecordWriter secondWriter = createRecordWriter(recordFile);

        try {
            recordWriter.openWriter();
            assertTrue(recordWriter.isOpen());
            secondWriter.openWriter();
            assertFalse(recordWriter.isOpen());
        } finally {
            recordWriter.closeWriter();
            secondWriter.closeWriter();
        }
    }

    @Test
    public void testBigRecordStorage() throws JasDBStorageException, FileException, IOException {
        String htmlData = ResourceUtil.getContent("datasets/htmlpage.data", "UTF-8");

        RecordWriter recordWriter = createRecordWriter(new File(BaseTest.tmpDir, "bigstore.pjs"));
        recordWriter.openWriter();
        try {
            UUIDKey documentKey = new UUIDKey(UUID.randomUUID());
            recordWriter.writeRecord(documentKey, RecordStreamUtil.toStream(htmlData));

            RecordResult result = recordWriter.readRecord(documentKey);
            assertNotNull(result);
			assertEquals("Html should remain intact", htmlData, RecordStreamUtil.toString(result));
        } finally {
            recordWriter.closeWriter();
        }
    }

    @Test
    public void testRecordRead() throws JasDBStorageException, IOException {
        int testSize = 100;
        RecordWriter recordWriter = createRecordWriter(new File(BaseTest.tmpDir, "teststore.pjs"));
        recordWriter.openWriter();

        try {
            List<UUIDKey> documentIds = createTestRecords(recordWriter, testSize);
            int counter = 0;
            for(UUIDKey documentId : documentIds) {
                RecordResult result = recordWriter.readRecord(documentId);
                assertNotNull("Record: " + documentId + " was null", result);

                String recordContents = RecordStreamUtil.toString(result);
                assertNotNull("There should be record contents", recordContents);
                assertEquals("Unexpected record content", SOME_RECORD_CONTENTS + counter, recordContents);
                counter++;
            }
        } finally {
            recordWriter.closeWriter();
        }
    }

    @Test
    public void testRecordRemove() throws JasDBStorageException {
        int testSize = 100;
        RecordWriter recordWriter = createRecordWriter(new File(BaseTest.tmpDir, "teststore.pjs"));
        recordWriter.openWriter();
        assertTrue(recordWriter.isOpen());
        try {
            List<UUIDKey> documentIds = createTestRecords(recordWriter, testSize);
            assertRecordsFound(recordWriter.readAllRecords(), testSize);

            int expected = testSize;
            for(UUIDKey documentId : documentIds) {
                assertRecordsFound(recordWriter.readAllRecords(), expected);

                recordWriter.removeRecord(documentId);
                expected--;

                assertRecordsFound(recordWriter.readAllRecords(), expected);
            }
        } finally {
            recordWriter.closeWriter();
        }
    }

    @Test
    public void testReadLimit() throws JasDBStorageException {
        int testSize = 1000;
        int testLimitSize = 100;
        RecordWriter recordWriter = createRecordWriter(new File(BaseTest.tmpDir, "teststore.pjs"));
        recordWriter.openWriter();
        assertTrue(recordWriter.isOpen());

        try {
            createTestRecords(recordWriter, testSize);

            assertRecordsFound(recordWriter.readAllRecords(testLimitSize), testLimitSize);
        } finally {
            recordWriter.closeWriter();
        }
    }

    private void assertRecordsFound(RecordIterator recordIterator, int expected) throws JasDBStorageException {
        int counter = 0;
        for(RecordResult result : recordIterator) {
            counter++;
            assertTrue("There should be content in the result", result.isRecordFound());
        }
        assertEquals("Unexpected amount of records found in storage", expected, counter);
    }

    private List<UUIDKey> createTestRecords(RecordWriter recordWriter, int testSize) throws JasDBStorageException {
        return createTestRecords(SOME_RECORD_CONTENTS, recordWriter, testSize);
    }

    private List<UUIDKey> createTestRecords(String recordContent, RecordWriter recordWriter, int testSize) throws JasDBStorageException {
        List<UUIDKey> recordPointers = new ArrayList<UUIDKey>(testSize);
        for(int i=0; i<testSize; i++) {
            UUIDKey documentKey = new UUIDKey(UUID.randomUUID());

            recordWriter.writeRecord(documentKey, RecordStreamUtil.toStream(recordContent + i));
            assertEquals(i + 1, recordWriter.getSize());
            recordPointers.add(documentKey);
        }
        LOG.info("Finished creating test records");

        return recordPointers;
    }
}
