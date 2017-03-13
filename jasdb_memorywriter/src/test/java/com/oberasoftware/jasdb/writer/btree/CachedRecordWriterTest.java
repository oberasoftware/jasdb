package com.oberasoftware.jasdb.writer.btree;

import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.storage.RecordIterator;
import com.oberasoftware.jasdb.api.storage.RecordResult;
import com.oberasoftware.jasdb.api.storage.RecordWriter;
import com.oberasoftware.jasdb.api.storage.ClonableDataStream;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Renze de Vries
 */
public class CachedRecordWriterTest {
    private static final Logger LOG = LoggerFactory.getLogger(CachedRecordWriterTest.class);

    @Test
    public void testReadRecords() throws JasDBStorageException, ConfigurationException {
//        MockWriter recordWriter = new MockWriter();
//        CacheManager cacheManager = new CacheManager();
//        CachedRecordWriter cachedRecordWriter = new CachedRecordWriter(cacheManager, "bag", recordWriter);
//
//        Map<String, String> cachingAttributes = new HashMap<String, String>();
//        cachingAttributes.put("Enabled", "true");
//        cachingAttributes.put("Value", "2000000");
//        ManualConfiguration manualConfiguration = new ManualConfiguration("caching", cachingAttributes);
//        ManualConfiguration maxItemsConfig = new ManualConfiguration("", cachingAttributes);
//        manualConfiguration.addChildConfiguration("./Property[@Name='MaxItems']", maxItemsConfig);
//        cacheManager.configure(manualConfiguration);
//        cachedRecordWriter.openWriter();
//
//
//        int nrRecords = 1000000;
//
//        for(long i=0; i<nrRecords; i++) {
//            RecordResultImpl result = cachedRecordWriter.readRecord(i);
//            assertEquals("record" + i, result.getContents());
//            assertEquals(i, result.getCurrentRecordPointer());
//            assertTrue(recordWriter.isTriggered());
//            recordWriter.reset();
//        }
//
//        for(long i=0; i<nrRecords; i++) {
//            RecordResultImpl result = cachedRecordWriter.readRecord(i);
//            assertEquals("record" + i, result.getContents());
//            assertEquals(i, result.getCurrentRecordPointer());
//            assertFalse(recordWriter.isTriggered());
//        }
    }

    private class MockWriter implements RecordWriter<UUIDKey> {
        private boolean triggered = false;

        public boolean isTriggered() {
            return triggered;
        }

        public void reset() {
            triggered = false;
        }

        @Override
        public long getDiskSize() throws JasDBStorageException {
            return 0;
        }

        @Override
        public long getSize() throws JasDBStorageException {
            return 0;
        }

        @Override
        public void openWriter() throws JasDBStorageException {

        }

        @Override
        public void closeWriter() throws JasDBStorageException {

        }

        @Override
        public void flush() throws JasDBStorageException {

        }

        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public RecordIterator readAllRecords() throws JasDBStorageException {
            return null;
        }

        @Override
        public RecordIterator readAllRecords(int limit) throws JasDBStorageException {
            return null;
        }

        @Override
        public RecordResult readRecord(UUIDKey key) throws JasDBStorageException {
            triggered = true;
            return new BtreeRecordResult();
        }

        @Override
        public void writeRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {

        }

        @Override
        public void removeRecord(UUIDKey documentId) throws JasDBStorageException {

        }

        @Override
        public void updateRecord(UUIDKey documentId, ClonableDataStream dataStream) throws JasDBStorageException {

        }
    }
}
