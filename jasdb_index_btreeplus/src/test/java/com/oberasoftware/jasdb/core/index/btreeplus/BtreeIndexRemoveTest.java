package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.core.statistics.StatisticsMonitor;
import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIterator;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author Renze de Vries
 */
public class BtreeIndexRemoveTest extends IndexBaseTest {
    private final Logger log = LoggerFactory.getLogger(BtreeIndexRemoveTest.class);

    @Before
    public void setup() {
        cleanData();
    }

    @After
    public void tearDown() {
        cleanData();
    }

    @Test
    public void testLongReadDeleteIndex() throws Exception {
        int indexSize = 28;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());
        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 5, index);
            removeKey(availableIndexes, 13, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 14, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 0, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 20, index);
            removeKey(availableIndexes, 21, index);
            removeKey(availableIndexes, 22, index);
            removeKey(availableIndexes, 24, index);
            removeKey(availableIndexes, 27, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 8, index);
            removeKey(availableIndexes, 11, index);
            removeKey(availableIndexes, 3, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 6, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 26, index);
            removeKey(availableIndexes, 12, index);
            removeKey(availableIndexes, 4, index);
            removeKey(availableIndexes, 23, index);
            removeKey(availableIndexes, 9, index);
            removeKey(availableIndexes, 10, index);

            for(int i=0; i<indexSize; i++) {
                IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new LongKey(i)), new SearchLimit());
                Assert.assertTrue("There should no longer be a result", result.isEmpty());
            }
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteRandomIndex() throws Exception {
        int indexSize = 100000;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());
        index.flushIndex();
        index.close();

        StatisticsMonitor.enableStatistics();

        List<Integer> removeSequence = new ArrayList<>();
        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        assertBlocks(index.getLockManager(), index.getPersister(), index.getRootBlock(), index.getPersister().getMaxKeys(), index.getPersister().getMinKeys(), -1);
        try {
            Random rnd = new Random(System.currentTimeMillis());
            int attempts = 0;
            log.debug("Starting remove operations test");
            long total = 0;
            while(!availableIndexes.isEmpty() && attempts < indexSize) {
                attempts++;
                int randomDeleteIndex = rnd.nextInt(availableIndexes.size());
                Integer deleteIndex = availableIndexes.get(randomDeleteIndex);
                removeSequence.add(deleteIndex);

                log.debug("Checking if key: {} can be found pre-remove operation", deleteIndex);
                IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new LongKey(deleteIndex)), new SearchLimit());
                Assert.assertFalse("There should be a result for: " + deleteIndex, result.isEmpty());

                log.debug("Starting delete operation for key: {}", deleteIndex);
                long start = System.nanoTime();
                index.removeFromIndex(new LongKey(deleteIndex));
//                assertBlocks(index.getPersister(), index.getRootBlock(), index.getPersister().getMaxKeys(), index.getPersister().getMinKeys(), -1);
                long end = System.nanoTime();
                double passed = (end - start) / (double)(1000 * 1000);
                total += end - start;
                log.debug("Delete operation for key: {} took: {} ms. remaining: {}", new Object[] {deleteIndex, passed, availableIndexes.size()});

                log.debug(index.toString());

                log.debug("Performing check to see if key is still present: {}", deleteIndex);
                result = index.searchIndex(new EqualsCondition(new LongKey(deleteIndex)), new SearchLimit());
                Assert.assertTrue("There should no longer be a result", result.isEmpty());

                if(result.isEmpty()) {
                    availableIndexes.remove(randomDeleteIndex);
                }
            }
            log.info("There are: {} remaining items in the index", availableIndexes.size());
            log.info("Average remove operation took: {}", (total / indexSize));
            StatisticsMonitor.logStats(TimeUnit.NANOSECONDS);
        } finally {
            log.info("Finishing test, flushing index and closing");
            index.flushIndex();
            index.close();

            StringBuilder removeSequenceStringBuilder = new StringBuilder();
            for(Integer sequenceItem : removeSequence) {
                removeSequenceStringBuilder.append("removeKey(availableIndexes, ").append(sequenceItem).append(", index);\n");
            }
            log.debug("Code for Remove sequence: \n{}", removeSequenceStringBuilder);
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure1() throws Exception {
        int indexSize = 28;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 10, index);
        } finally {
            log.debug("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure2() throws Exception {
        int indexSize = 28;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());
        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 14, index);
            removeKey(availableIndexes, 10, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure3() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 90, index);
            removeKey(availableIndexes, 2, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure4() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 0, index);
            removeKey(availableIndexes, 46, index);
            removeKey(availableIndexes, 86, index);
            removeKey(availableIndexes, 95, index);
            removeKey(availableIndexes, 84, index);
            removeKey(availableIndexes, 48, index);
            removeKey(availableIndexes, 99, index);
            removeKey(availableIndexes, 80, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 49, index);
            removeKey(availableIndexes, 42, index);
            removeKey(availableIndexes, 94, index);
            removeKey(availableIndexes, 21, index);
            removeKey(availableIndexes, 98, index);
            removeKey(availableIndexes, 11, index);
            removeKey(availableIndexes, 70, index);
            removeKey(availableIndexes, 25, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure5() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 80, index);
            removeKey(availableIndexes, 83, index);
            removeKey(availableIndexes, 32, index);
            removeKey(availableIndexes, 63, index);
            removeKey(availableIndexes, 12, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 87, index);
            removeKey(availableIndexes, 11, index);
            removeKey(availableIndexes, 47, index);
            removeKey(availableIndexes, 45, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 14, index);
            removeKey(availableIndexes, 38, index);
            removeKey(availableIndexes, 30, index);
            removeKey(availableIndexes, 36, index);
            removeKey(availableIndexes, 5, index);
            removeKey(availableIndexes, 64, index);
            removeKey(availableIndexes, 43, index);
            removeKey(availableIndexes, 94, index);
            removeKey(availableIndexes, 86, index);
            removeKey(availableIndexes, 73, index);
            removeKey(availableIndexes, 65, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 90, index);
            removeKey(availableIndexes, 33, index);
            removeKey(availableIndexes, 51, index);
            removeKey(availableIndexes, 79, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 76, index);
            removeKey(availableIndexes, 42, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 50, index);
            removeKey(availableIndexes, 96, index);
            removeKey(availableIndexes, 9, index);
            removeKey(availableIndexes, 70, index);
            removeKey(availableIndexes, 21, index);
            removeKey(availableIndexes, 98, index);
            removeKey(availableIndexes, 55, index);
            removeKey(availableIndexes, 97, index);
            removeKey(availableIndexes, 68, index);
            removeKey(availableIndexes, 27, index);
            removeKey(availableIndexes, 74, index);
            removeKey(availableIndexes, 48, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 88, index);
            removeKey(availableIndexes, 84, index);
            removeKey(availableIndexes, 39, index);
            removeKey(availableIndexes, 22, index);
            removeKey(availableIndexes, 67, index);
            removeKey(availableIndexes, 71, index);
            removeKey(availableIndexes, 52, index);
            removeKey(availableIndexes, 89, index);
            removeKey(availableIndexes, 24, index);
            removeKey(availableIndexes, 82, index);
            removeKey(availableIndexes, 8, index);
            removeKey(availableIndexes, 49, index);
            removeKey(availableIndexes, 78, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 95, index);
            removeKey(availableIndexes, 3, index);
            removeKey(availableIndexes, 62, index);
            removeKey(availableIndexes, 37, index);
            removeKey(availableIndexes, 81, index);
            removeKey(availableIndexes, 13, index);
            removeKey(availableIndexes, 4, index);
            removeKey(availableIndexes, 77, index);
            removeKey(availableIndexes, 34, index);
            removeKey(availableIndexes, 91, index);
            removeKey(availableIndexes, 23, index);
            removeKey(availableIndexes, 58, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure6() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 27, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 26, index);
            removeKey(availableIndexes, 68, index);
            removeKey(availableIndexes, 78, index);
            removeKey(availableIndexes, 61, index);
            removeKey(availableIndexes, 10, index);
            removeKey(availableIndexes, 42, index);
            removeKey(availableIndexes, 23, index);
            removeKey(availableIndexes, 99, index);
            removeKey(availableIndexes, 56, index);
            removeKey(availableIndexes, 87, index);
            removeKey(availableIndexes, 83, index);
            removeKey(availableIndexes, 8, index);
            removeKey(availableIndexes, 82, index);
            removeKey(availableIndexes, 36, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 3, index);
            removeKey(availableIndexes, 73, index);
            removeKey(availableIndexes, 52, index);
            removeKey(availableIndexes, 45, index);
            removeKey(availableIndexes, 50, index);
            removeKey(availableIndexes, 80, index);
            removeKey(availableIndexes, 69, index);
            removeKey(availableIndexes, 43, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 76, index);
            removeKey(availableIndexes, 35, index);
            removeKey(availableIndexes, 88, index);
            removeKey(availableIndexes, 39, index);
            removeKey(availableIndexes, 59, index);
            removeKey(availableIndexes, 46, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 71, index);
            removeKey(availableIndexes, 9, index);
            removeKey(availableIndexes, 81, index);
            removeKey(availableIndexes, 92, index);
            removeKey(availableIndexes, 49, index);
            removeKey(availableIndexes, 65, index);

        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure7() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 64, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 21, index);
            removeKey(availableIndexes, 90, index);
            removeKey(availableIndexes, 67, index);
            removeKey(availableIndexes, 49, index);
            removeKey(availableIndexes, 47, index);
            removeKey(availableIndexes, 51, index);
            removeKey(availableIndexes, 38, index);
            removeKey(availableIndexes, 92, index);
            removeKey(availableIndexes, 79, index);
            removeKey(availableIndexes, 83, index);
            removeKey(availableIndexes, 6, index);
            removeKey(availableIndexes, 93, index);
            removeKey(availableIndexes, 98, index);
            removeKey(availableIndexes, 94, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 39, index);
            removeKey(availableIndexes, 62, index);
            removeKey(availableIndexes, 48, index);
            removeKey(availableIndexes, 57, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 33, index);
            removeKey(availableIndexes, 35, index);
            removeKey(availableIndexes, 69, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 71, index);
            removeKey(availableIndexes, 81, index);
            removeKey(availableIndexes, 41, index);
            removeKey(availableIndexes, 26, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 30, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 91, index);
            removeKey(availableIndexes, 28, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 63, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure8() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 59, index);
            removeKey(availableIndexes, 6, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 63, index);
            removeKey(availableIndexes, 22, index);
            removeKey(availableIndexes, 93, index);
            removeKey(availableIndexes, 48, index);
            removeKey(availableIndexes, 38, index);
            removeKey(availableIndexes, 65, index);
            removeKey(availableIndexes, 71, index);
            removeKey(availableIndexes, 4, index);
            removeKey(availableIndexes, 80, index);
            removeKey(availableIndexes, 85, index);
            removeKey(availableIndexes, 67, index);
            removeKey(availableIndexes, 23, index);
            removeKey(availableIndexes, 97, index);
            removeKey(availableIndexes, 88, index);
            removeKey(availableIndexes, 51, index);
            removeKey(availableIndexes, 83, index);
            removeKey(availableIndexes, 66, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 40, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure9() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 55, index);
            removeKey(availableIndexes, 29, index);
            removeKey(availableIndexes, 52, index);
            removeKey(availableIndexes, 56, index);
            removeKey(availableIndexes, 70, index);
            removeKey(availableIndexes, 20, index);
            removeKey(availableIndexes, 38, index);
            removeKey(availableIndexes, 66, index);
            removeKey(availableIndexes, 98, index);
            removeKey(availableIndexes, 42, index);
            removeKey(availableIndexes, 95, index);
            removeKey(availableIndexes, 4, index);
            removeKey(availableIndexes, 31, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 78, index);
            removeKey(availableIndexes, 36, index);
            removeKey(availableIndexes, 76, index);
            removeKey(availableIndexes, 75, index);
            removeKey(availableIndexes, 90, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 67, index);
            removeKey(availableIndexes, 27, index);
            removeKey(availableIndexes, 88, index);
            removeKey(availableIndexes, 3, index);
            removeKey(availableIndexes, 14, index);
            removeKey(availableIndexes, 37, index);
            removeKey(availableIndexes, 39, index);
            removeKey(availableIndexes, 59, index);
            removeKey(availableIndexes, 13, index);
            removeKey(availableIndexes, 6, index);
            removeKey(availableIndexes, 45, index);
            removeKey(availableIndexes, 99, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 79, index);
            removeKey(availableIndexes, 92, index);
            removeKey(availableIndexes, 65, index);
            removeKey(availableIndexes, 74, index);
            removeKey(availableIndexes, 46, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 8, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 47, index);
            removeKey(availableIndexes, 84, index);
            removeKey(availableIndexes, 54, index);
            removeKey(availableIndexes, 83, index);
            removeKey(availableIndexes, 53, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 71, index);
            removeKey(availableIndexes, 93, index);
            removeKey(availableIndexes, 11, index);
            removeKey(availableIndexes, 22, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 80, index);
            removeKey(availableIndexes, 87, index);
            removeKey(availableIndexes, 24, index);
            removeKey(availableIndexes, 26, index);
            removeKey(availableIndexes, 10, index);
            removeKey(availableIndexes, 69, index);
            removeKey(availableIndexes, 28, index);
            removeKey(availableIndexes, 21, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 43, index);
            removeKey(availableIndexes, 68, index);
            removeKey(availableIndexes, 63, index);
            removeKey(availableIndexes, 44, index);
            removeKey(availableIndexes, 61, index);
            removeKey(availableIndexes, 33, index);
            removeKey(availableIndexes, 30, index);
            removeKey(availableIndexes, 85, index);
            removeKey(availableIndexes, 94, index);
            removeKey(availableIndexes, 89, index);
            removeKey(availableIndexes, 12, index);
            removeKey(availableIndexes, 58, index);
            removeKey(availableIndexes, 60, index);
            removeKey(availableIndexes, 23, index);
            removeKey(availableIndexes, 77, index);
            removeKey(availableIndexes, 97, index);
            removeKey(availableIndexes, 81, index);
            removeKey(availableIndexes, 9, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 72, index);
            removeKey(availableIndexes, 32, index);
            removeKey(availableIndexes, 41, index);
            removeKey(availableIndexes, 5, index);
            removeKey(availableIndexes, 82, index);
            removeKey(availableIndexes, 64, index);
            removeKey(availableIndexes, 86, index);
            removeKey(availableIndexes, 35, index);
            removeKey(availableIndexes, 50, index);
            removeKey(availableIndexes, 62, index);
            removeKey(availableIndexes, 51, index);
            removeKey(availableIndexes, 34, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure10() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 68, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 43, index);
            removeKey(availableIndexes, 98, index);
            removeKey(availableIndexes, 0, index);
            removeKey(availableIndexes, 64, index);
            removeKey(availableIndexes, 30, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 84, index);
            removeKey(availableIndexes, 78, index);
            removeKey(availableIndexes, 73, index);
            removeKey(availableIndexes, 29, index);
            removeKey(availableIndexes, 85, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 76, index);
            removeKey(availableIndexes, 72, index);
            removeKey(availableIndexes, 48, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 95, index);
            removeKey(availableIndexes, 67, index);
            removeKey(availableIndexes, 55, index);
            removeKey(availableIndexes, 82, index);
            removeKey(availableIndexes, 44, index);
            removeKey(availableIndexes, 24, index);
            removeKey(availableIndexes, 13, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 47, index);
            removeKey(availableIndexes, 34, index);
            removeKey(availableIndexes, 56, index);
            removeKey(availableIndexes, 35, index);
            removeKey(availableIndexes, 75, index);
            removeKey(availableIndexes, 87, index);
            removeKey(availableIndexes, 10, index);
            removeKey(availableIndexes, 11, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 45, index);
            removeKey(availableIndexes, 53, index);
            removeKey(availableIndexes, 21, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure11() throws Exception {
        int indexSize = 100;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 47, index);
            removeKey(availableIndexes, 86, index);
            removeKey(availableIndexes, 49, index);
            removeKey(availableIndexes, 25, index);
            removeKey(availableIndexes, 74, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 28, index);
            removeKey(availableIndexes, 40, index);
            removeKey(availableIndexes, 17, index);
            removeKey(availableIndexes, 66, index);
            removeKey(availableIndexes, 51, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexFailure12() throws Exception {
        int indexSize = 1000;
        List<Integer> availableIndexes = new ArrayList<>();
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            availableIndexes.add(i);
        }
        log.debug("Index after insert: \n{}", index.toString());
        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 663, index);
            removeKey(availableIndexes, 867, index);
            removeKey(availableIndexes, 375, index);
            removeKey(availableIndexes, 956, index);
            removeKey(availableIndexes, 136, index);
            removeKey(availableIndexes, 249, index);
            removeKey(availableIndexes, 657, index);
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexParentNodes() throws Exception {
        int indexSize = 28;
        List<Integer> availableIndexes = new ArrayList<>();

        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            long startInsert = System.nanoTime();
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);
            availableIndexes.add(i);
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        try {
            log.debug("Starting remove operations test");
            index.removeFromIndex(new LongKey(5));
            availableIndexes.remove(Integer.valueOf(5));
            log.debug(index.toString());

            index.removeFromIndex(new LongKey(13));
            availableIndexes.remove(Integer.valueOf(13));
            log.debug(index.toString());

            index.removeFromIndex(new LongKey(14));
            availableIndexes.remove(Integer.valueOf(14));
            log.debug(index.toString());

            index.removeFromIndex(new LongKey(9));
            availableIndexes.remove(Integer.valueOf(9));
            log.debug(index.toString());

            assertIndexKeysPresent(availableIndexes, index);
            log.info("There are: {} remaining items in the index", availableIndexes.size());
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    @Test
    public void testLongReadDeleteIndexRootMerge() throws Exception {
        int indexSize = 20;
        List<Integer> availableIndexes = new ArrayList<>();

        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("somekey", new LongKeyType()), new SimpleIndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            long startInsert = System.nanoTime();
            index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);
            availableIndexes.add(i);
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        index.flushIndex();
        index.close();

        index = new BTreeIndex(new File(tmpDir, "indexbag_longdelete.idx"), keyInfo);
        index.openIndex();
        try {
            log.debug("Freshly loaded index: \n{}", index.toString());
            log.debug("Starting remove operations test");
            removeKey(availableIndexes, 5, index);
            removeKey(availableIndexes, 13, index);
            removeKey(availableIndexes, 14, index);
            removeKey(availableIndexes, 16, index);
            removeKey(availableIndexes, 18, index);
            removeKey(availableIndexes, 7, index);
            removeKey(availableIndexes, 8, index);
            removeKey(availableIndexes, 9, index);
            removeKey(availableIndexes, 11, index);

            removeKey(availableIndexes, 4, index);
            removeKey(availableIndexes, 1, index);
            removeKey(availableIndexes, 2, index);
            removeKey(availableIndexes, 3, index);
            removeKey(availableIndexes, 6, index);
            removeKey(availableIndexes, 10, index);
            removeKey(availableIndexes, 12, index);
            removeKey(availableIndexes, 0, index);
            removeKey(availableIndexes, 15, index);
            removeKey(availableIndexes, 19, index);
            removeKey(availableIndexes, 17, index);

            assertIndexKeysPresent(availableIndexes, index);
            log.info("There are: {} remaining items in the index", availableIndexes.size());
        } finally {
            log.info("Finishing test, flusing index and closing");
            index.flushIndex();
            index.close();
        }
    }

    private void removeKey(List<Integer> availableLongKeys, int key, BTreeIndex index) throws JasDBStorageException {
        log.debug("Doing remove of key: {}", key);
        index.removeFromIndex(new LongKey(key));
        availableLongKeys.remove((Integer)key);
        log.trace("Index after remove of: {}", key);

        assertBlocks(index.getLockManager(), index.getPersister(), index.getRootBlock(), index.getPersister().getMaxKeys(), index.getPersister().getMinKeys(), -1);

        assertIndexKeysPresent(availableLongKeys, index);
    }

}
