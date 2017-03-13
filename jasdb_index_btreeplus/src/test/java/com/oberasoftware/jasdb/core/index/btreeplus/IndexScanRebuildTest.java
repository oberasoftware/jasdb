package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.session.IndexableItem;
import com.oberasoftware.jasdb.api.exceptions.ConfigurationException;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.api.index.IndexScanReport;
import com.oberasoftware.jasdb.api.index.IndexState;
import com.oberasoftware.jasdb.api.index.ScanIntent;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
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

        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("field1", new LongKeyType()), new SimpleIndexField("RECORD_POINTER", new LongKeyType()));
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
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("field1", new LongKeyType()), new SimpleIndexField("RECORD_POINTER", new LongKeyType()));
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
