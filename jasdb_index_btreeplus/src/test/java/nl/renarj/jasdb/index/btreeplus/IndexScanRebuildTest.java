package nl.renarj.jasdb.index.btreeplus;

import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.IndexScanReport;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.ScanIntent;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Renze de Vries
 */
public class IndexScanRebuildTest extends IndexBaseTest {
    private Logger log = LoggerFactory.getLogger(IndexScanRebuildTest.class);

    @After
    public void tearDown() throws ConfigurationException {
        assertDelete(new File(tmpDir, "indexbag_field1.idx"));
    }

    @Before
    public void setup() {
        assertDelete(new File(tmpDir, "indexbag_field1.idx"));
    }

    @Test
    public void testIndexScan() throws JasDBStorageException {
        int testSize = 100000;
        String testField = "field1";
        List<IndexableItem> indexableItemList = new ArrayList<>(testSize);
        List<Integer> insertedKeys = new ArrayList<>(testSize);
        for(int i=0; i<testSize; i++) {
            indexableItemList.add(new MockIndexableItem(testField, i, i + 1000));
            insertedKeys.add(i);
        }

        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("field1", new LongKeyType()), new IndexField("RECORD_POINTER", new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_field1.idx"), keyInfo);
        IndexScanReport report = index.scan(ScanIntent.RESCAN, indexableItemList.iterator());
        assertEquals("Expected a 0% complete index", 0, report.getCompleteness());
        assertEquals("Expected invalid state", IndexState.INVALID, report.getState());

        long start = System.currentTimeMillis();
        index.rebuildIndex(indexableItemList.iterator());
        long end = System.currentTimeMillis();
        log.info("Completed index rebuild in {} ms.", (end - start));
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_field1.idx"), keyInfo);
        try {
            start = System.currentTimeMillis();
            report = index.scan(ScanIntent.RESCAN, indexableItemList.iterator());
            end = System.currentTimeMillis();
            log.info("Completed index scan in: {} ms.", (end - start));
            assertEquals("Expected a 100% complete index", 100, report.getCompleteness());
            assertEquals("Expected ok state", IndexState.OK, report.getState());

            assertIndexKeysPresent(insertedKeys, index);
        } finally {
            index.close();
        }
    }

    @Test
    public void textIncompleteIndex() throws JasDBStorageException {
        int testSize = 100000;
        int initialFill = testSize / 4;
        String testField = "field1";
        List<IndexableItem> indexableItemList = new ArrayList<>(testSize);
        List<Integer> insertedKeys = new ArrayList<>(testSize);
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("field1", new LongKeyType()), new IndexField("RECORD_POINTER", new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_field1.idx"), keyInfo);
        try {
            for(int i=0; i<testSize; i++) {
                indexableItemList.add(new MockIndexableItem(testField, i, i + 1000));
                insertedKeys.add(i);

                if(i < initialFill) {
                    index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(i + 1000)));
                }
            }
        } finally {
            index.close();
        }

        index = new BTreeIndex(new File(tmpDir, "indexbag_field1.idx"), keyInfo);
        try {
            IndexScanReport report = index.scan(ScanIntent.RESCAN, indexableItemList.iterator());
            assertEquals("Expected a 25% complete index", 25, report.getCompleteness());
            assertEquals("Expected invalid state", IndexState.INVALID, report.getState());
            long start = System.currentTimeMillis();
            index.rebuildIndex(indexableItemList.iterator());
            long end = System.currentTimeMillis();
            log.info("Completed index rebuild in {} ms.", (end - start));
        } finally {
            index.close();
        }

        index = new BTreeIndex(new File(tmpDir, "indexbag_field1.idx"), keyInfo);
        try {
            long start = System.currentTimeMillis();
            IndexScanReport report = index.scan(ScanIntent.RESCAN, indexableItemList.iterator());
            long end = System.currentTimeMillis();
            log.info("Completed index scan in: {} ms.", (end - start));
            assertEquals("Expected a 100% complete index", 100, report.getCompleteness());
            assertEquals("Expected ok state", IndexState.OK, report.getState());

            assertIndexKeysPresent(insertedKeys, index);
        } finally {
            index.close();
        }
    }
}
