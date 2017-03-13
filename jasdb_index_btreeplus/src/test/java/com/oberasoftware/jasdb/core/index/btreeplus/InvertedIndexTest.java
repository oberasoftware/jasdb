package com.oberasoftware.jasdb.core.index.btreeplus;

import com.google.common.collect.Lists;
import com.oberasoftware.jasdb.api.index.Index;
import com.oberasoftware.jasdb.api.index.IndexState;
import com.oberasoftware.jasdb.core.index.keys.CompositeKey;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.UUIDKeyType;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIterator;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.core.index.query.EqualsCondition;
import com.oberasoftware.jasdb.api.index.IndexField;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class InvertedIndexTest extends IndexBaseTest {
	private static final Logger log = LoggerFactory.getLogger(InvertedIndexTest.class);
    private static final Random rnd = new Random(System.nanoTime());
	
	@After
	public void tearDown() {
		assertDelete(new File(tmpDir, "inverted.idx"));
		assertDelete(new File(tmpDir, "inverted.idxm"));
	}

    @Before
    public void setup() {
        assertDelete(new File(tmpDir, "inverted.idx"));
        assertDelete(new File(tmpDir, "inverted.idxm"));
    }
	
	@Test
	public void testInvertedIndex() throws Exception {
		int NUMBER_ENTITIES = 20000;
		File indexLocation = new File(tmpDir, "inverted.idx");
        Map<Integer, Integer> keyAmounts = new HashMap<>();

		KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(new SimpleIndexField("field1", new StringKeyType()), new SimpleIndexField("__ID", new UUIDKeyType())), new ArrayList<IndexField>());
		Index invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        try {
            long total = 0;
            for(int i=0; i<NUMBER_ENTITIES; i++) {
                int key = rnd.nextInt(8);
                changeAmount(keyAmounts, key, true);

                long start = System.nanoTime();
                CompositeKey compositeKey = new CompositeKey();
                compositeKey.addKey(keyInfo.getKeyNameMapper(), "field1", new StringKey("key" + key));
                compositeKey.addKey(keyInfo.getKeyNameMapper(), "__ID", new UUIDKey(System.currentTimeMillis(), i + 1000));

                invertedIndex.insertIntoIndex(compositeKey);
                long end = System.nanoTime();
                total += (end - start);
            }
            log.info("Average insert time: {}", (total / NUMBER_ENTITIES));
        } finally {
            invertedIndex.close();
        }
		
		Index index = new BTreeIndex(indexLocation, keyInfo);
		try {
            index.openIndex();
            assertIndex(keyAmounts, index);

            int runs = 100;
            long total = 0;
            for(int i=0; i<runs; i++) {
                long averageSearch = assertIndex(keyAmounts, index);
                log.debug("Run: {} took average: {} ns.", i, averageSearch);
                total += averageSearch;
            }
            long average = total / runs;
            log.info("Average retrieval of {} runs was {} ns.", runs, average);
        } finally {
			index.close();
		}
	}

    @Test
    public void testInvertedIndexCompoundKey() throws Exception {
        int NUMBER_ENTITIES = 20000;
        int maxKey = 8;
        int maxAge = 100;
        File indexLocation = new File(tmpDir, "inverted.idx");
        Map<String, Integer> keyAmounts = new HashMap<>();

        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                    new SimpleIndexField("field", new StringKeyType(200)),
                    new SimpleIndexField("age", new LongKeyType()),
                    new SimpleIndexField("__ID", new UUIDKeyType())
            ),new ArrayList<IndexField>()
        );
        Index index = new BTreeIndex(indexLocation, keyInfo);
        KeyNameMapper mapper = keyInfo.getKeyNameMapper();
        try {
            long total = 0;
            for(int i=0; i<NUMBER_ENTITIES; i++) {
                int key = rnd.nextInt(maxKey);
                int age = rnd.nextInt(maxAge);
                String uniqueKey = "key" + key + "_" + age;
                changeAmount(keyAmounts, uniqueKey, true);

                CompositeKey insertKey = new CompositeKey();
                insertKey.addKey(mapper, "field", new StringKey("key" + key))
                        .addKey(mapper, "age", new LongKey(age))
                        .addKey(keyInfo.getKeyNameMapper(), "__ID", new UUIDKey(System.currentTimeMillis(), i + 1000));

                long start = System.nanoTime();
                index.insertIntoIndex(insertKey);
                long end = System.nanoTime();
                total += (end - start);
            }
            log.info("Average insert time: {}", (total / NUMBER_ENTITIES));
        } finally {
            index.close();
        }

        index = new BTreeIndex(indexLocation, keyInfo);
        try {
            for(int key=0; key<maxKey; key++) {
                for(int age=0; age<maxAge; age++) {
                    CompositeKey searchKey = new CompositeKey();
                    searchKey.addKey(mapper, "field", new StringKey("key" + key))
                            .addKey(mapper, "age", new LongKey(age));

                    IndexSearchResultIterator resultIterator = index.searchIndex(new EqualsCondition(searchKey), Index.NO_SEARCH_LIMIT);

                    String countKey = "key" + key + "_" + age;
                    if(keyAmounts.containsKey(countKey)) {
                        assertEquals((int) keyAmounts.get(countKey), resultIterator.size());
                    } else {
                        assertEquals(0, resultIterator.size());
                    }
                }
            }
        } finally {
            index.close();
        }
    }

    @Test
    public void testInsertCompoundKeyExtraInfo() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                new SimpleIndexField("field", new StringKeyType(200)),
                new SimpleIndexField("age", new LongKeyType()),
                new SimpleIndexField("RECORD_POINTER", new LongKeyType())
            ), new ArrayList<IndexField>()
        );

        File indexLocation = new File(tmpDir, "inverted.idx");
        Index index = new BTreeIndex(indexLocation, keyInfo);
        KeyNameMapper mapper = keyInfo.getKeyNameMapper();
        try {
            CompositeKey insertKey = new CompositeKey();
            insertKey.addKey(mapper, "field", new StringKey("mykey")).addKey(mapper, "age", new LongKey(29)).addKey(mapper, "RECORD_POINTER", new LongKey(100));
            index.insertIntoIndex(insertKey);

            insertKey = new CompositeKey();
            insertKey.addKey(mapper, "field", new StringKey("mykey")).addKey(mapper, "age", new LongKey(29)).addKey(mapper, "RECORD_POINTER", new LongKey(120));
            index.insertIntoIndex(insertKey);

            CompositeKey searchKey = new CompositeKey();
            searchKey.addKey(mapper, "field", new StringKey("mykey")).addKey(mapper, "age", new LongKey(29));

            IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(searchKey), Index.NO_SEARCH_LIMIT);
            assertEquals(2, result.size());
        } finally {
            index.close();
        }
    }

    @Test
    public void testCompoundKeyStringLongCompare() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                new SimpleIndexField("field", new StringKeyType(200)),
                new SimpleIndexField("age", new LongKeyType()),
                new SimpleIndexField("RECORD_POINTER", new LongKeyType())
        ), new ArrayList<IndexField>());

        File indexLocation = new File(tmpDir, "inverted.idx");
        Index invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        KeyNameMapper mapper = invertedIndex.getKeyInfo().getKeyNameMapper();
        try {
            CompositeKey insertKey = new CompositeKey();
            insertKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new LongKey(1)).addKey(mapper, "RECORD_POINTER", new LongKey(100));
            invertedIndex.insertIntoIndex(insertKey);

            insertKey = new CompositeKey();
            insertKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new LongKey(2)).addKey(mapper, "RECORD_POINTER", new LongKey(120));
            invertedIndex.insertIntoIndex(insertKey);

            insertKey = new CompositeKey();
            insertKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new LongKey(3)).addKey(mapper, "RECORD_POINTER", new LongKey(120));
            invertedIndex.insertIntoIndex(insertKey);

            CompositeKey searchKey = new CompositeKey();
            searchKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new StringKey("1"));
            IndexSearchResultIterator result = invertedIndex.searchIndex(new EqualsCondition(searchKey), Index.NO_SEARCH_LIMIT);
            assertEquals(1, result.size());

            searchKey = new CompositeKey();
            searchKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new StringKey("2"));
            result = invertedIndex.searchIndex(new EqualsCondition(searchKey), Index.NO_SEARCH_LIMIT);
            assertEquals(1, result.size());

            searchKey = new CompositeKey();
            searchKey.addKey(mapper, "field", new StringKey("amsterdam")).addKey(mapper, "age", new StringKey("3"));
            result = invertedIndex.searchIndex(new EqualsCondition(searchKey), Index.NO_SEARCH_LIMIT);
            assertEquals(1, result.size());
        } finally {
            invertedIndex.close();
        }
    }


    @Test
    public void testInvertedIndexRemoveKeys() throws Exception {
        int NUMBER_ENTITIES = 20000;
        int REMOVE_ENTITIES = 2000;
        File indexLocation = new File(tmpDir, "inverted.idx");
        Map<Integer, Integer> amounts = new HashMap<>();

        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                new SimpleIndexField("field1", new StringKeyType()),
                new SimpleIndexField("RECORD_POINTER", new LongKeyType())),
            new ArrayList<IndexField>());
        Index invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        KeyNameMapper mapper = keyInfo.getKeyNameMapper();
        try {
            int keyCounter = 0;
            for(int i=0; i<NUMBER_ENTITIES; i++) {
                CompositeKey insertKey = new CompositeKey();
                insertKey.addKey(mapper, "field1", new StringKey("key" + keyCounter));
                insertKey.addKey(mapper, "RECORD_POINTER", new LongKey(i + 1000));

                invertedIndex.insertIntoIndex(insertKey);
                changeAmount(amounts, keyCounter, true);

                keyCounter++;
                if(keyCounter > 10) keyCounter = 0;
            }
        } finally {
            invertedIndex.close();
        }

        invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        try {
            int keyCounter = 0;
            long totalTime = 0;
            for(int i=0; i<REMOVE_ENTITIES; i++) {
                long start = System.nanoTime();
                CompositeKey removeKey = new CompositeKey();
                removeKey.addKey(mapper, "field1", new StringKey("key" + keyCounter));
                removeKey.addKey(mapper, "RECORD_POINTER", new LongKey(i + 1000));

                invertedIndex.removeFromIndex(removeKey);
                long end = System.nanoTime();
                totalTime += (end - start);
                changeAmount(amounts, keyCounter, false);

                keyCounter++;
                if(keyCounter > 10) keyCounter = 0;
            }
            log.info("Average remove time: {}", (totalTime / REMOVE_ENTITIES));

            assertIndex(amounts, invertedIndex);
        } finally {
            invertedIndex.close();
        }
    }
    
    @Test
    public void testInvertedIndexInsertRemoveInsert() throws Exception {
    	int testSize = 1000;
    	int removeSize = 800;
    			
        File indexLocation = new File(tmpDir, "inverted.idx");
        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                new SimpleIndexField("field1", new StringKeyType()),
                new SimpleIndexField("RECORD_POINTER", new LongKeyType())),
                new ArrayList<IndexField>());
    	Index invertedIndex = new BTreeIndex(indexLocation, keyInfo);
    	try {
	    	for(int i=0; i<testSize; i++) {
                CompositeKey compositeKey = new CompositeKey();
                compositeKey.addKey(keyInfo.getKeyNameMapper(), "field1", new StringKey("key1")).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(i));
	    		invertedIndex.insertIntoIndex(compositeKey);
	    	}
	    	assertEquals("The size is unexpected", testSize, invertedIndex.searchIndex(new EqualsCondition(new StringKey("key1")), new SearchLimit()).size());

	    	for(int i=0; i<removeSize; i++) {
                CompositeKey compositeKey = new CompositeKey();
                compositeKey.addKey(keyInfo.getKeyNameMapper(), "field1", new StringKey("key1")).addKey(keyInfo.getKeyNameMapper(), "RECORD_POINTER", new LongKey(i));
	    		invertedIndex.removeFromIndex(compositeKey);
	    	}
	    	assertEquals("The size is unexpected", (testSize - removeSize), invertedIndex.searchIndex(new EqualsCondition(new StringKey("key1")), new SearchLimit()).size());
    	} finally {
    		invertedIndex.close();
    	}
    }

    @Test
    public void testInvertedIndexEmptyStringKey() throws Exception {
        File indexLocation = new File(tmpDir, "inverted.idx");
        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(
                new SimpleIndexField("field1", new StringKeyType()),
                new SimpleIndexField("RECORD_POINTER", new LongKeyType())),
                new ArrayList<IndexField>());
        Index invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        CompositeKey compositeKey = new CompositeKey();
        compositeKey.addKey(keyInfo.getKeyNameMapper(), "field1", new StringKey("")).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100));

        invertedIndex.insertIntoIndex(compositeKey);
        invertedIndex.close();

        assertEquals("State should be closed", IndexState.CLOSED, invertedIndex.getState());

        invertedIndex = new BTreeIndex(indexLocation, keyInfo);
        try {
            assertFalse(invertedIndex.searchIndex(new EqualsCondition(new StringKey("")), Index.NO_SEARCH_LIMIT).isEmpty());
        } finally {
            invertedIndex.close();
        }
    }
}
