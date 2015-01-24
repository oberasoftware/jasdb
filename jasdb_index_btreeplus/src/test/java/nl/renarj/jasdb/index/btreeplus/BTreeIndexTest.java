package nl.renarj.jasdb.index.btreeplus;

import com.google.common.collect.Lists;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.ManualConfiguration;
import nl.renarj.jasdb.core.caching.GlobalCachingMemoryManager;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.impl.CompositeKey;
import nl.renarj.jasdb.index.keys.impl.DataKey;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.types.DataKeyType;
import nl.renarj.jasdb.index.keys.types.LongKeyType;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.index.search.NotEqualsCondition;
import nl.renarj.jasdb.index.search.RangeCondition;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BTreeIndexTest extends IndexBaseTest {
	private Logger log = LoggerFactory.getLogger(BTreeIndexTest.class);
	
	@After
	public void tearDown() throws ConfigurationException {
        cleanData();
	}
	
	@Before
	public void setup() {
        cleanData();
	}

    @Test
    public void testConcurrentInsert() throws Exception {
        int nrThreads = 20;
        int nrRecords = 10000;

        Map<String, String> p = new HashMap<>();
        p.put("MaxMemory", "32m");
        GlobalCachingMemoryManager.getGlobalInstance().configure(new ManualConfiguration("Caching", p));
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new StringKeyType()), new IndexField("RECORD_POINTER", new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

        try {
            Map<Thread, BTreeIndexWriterThread> writers = new HashMap<>();
            for(int i=0; i<nrThreads; i++) {
                BTreeIndexWriterThread writer = new BTreeIndexWriterThread(index, i * nrRecords, nrRecords);
                Thread writerThread = new Thread(writer, "WriterThread" + i);
                writers.put(writerThread, writer);
            }

            for(Thread thread : writers.keySet()) {
                thread.start();
            }

            for(Map.Entry<Thread, BTreeIndexWriterThread> writerEntry : writers.entrySet()) {
                writerEntry.getKey().join();

                BTreeIndexWriterThread writer = writerEntry.getValue();
                log.info("Average time: {} ({} ms.)", writer.getAverage(), (double)writer.getAverage() / (double)(1000 * 1000));
                Assert.assertEquals("Expected no failures in insert operation", 0, writer.getFailures());
            }

            log.info("Using: {} bytes", GlobalCachingMemoryManager.getGlobalInstance().calculateMemorySize());
            log.info("Used memory: {}", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        } finally {
            index.closeIndex();
        }

    }

    private class BTreeIndexWriterThread implements Runnable {
        private Index index;
        private KeyNameMapper keyNameMapper;
        private long startRecordId;
        private int nrRecords;

        private int failures = 0;
        private long total = 0;

        public BTreeIndexWriterThread(Index index, int startRecordId, int nrRecords) {
            this.index = index;
            this.keyNameMapper = index.getKeyInfo().getKeyNameMapper();
            this.startRecordId = startRecordId;
            this.nrRecords = nrRecords;
        }

        public long getAverage() {
            return total / nrRecords;
        }

        public int getFailures() {
            return failures;
        }

        public void run() {
            long currentRecordId = startRecordId;
            for(int i=0; i<nrRecords; i++) {
                try {
                    long start = System.nanoTime();
                    index.insertIntoIndex(new StringKey("key" + currentRecordId).addKey(keyNameMapper,
                            "RECORD_POINTER", new LongKey(currentRecordId)));
                    long end = System.nanoTime();
                    total += (end - start);

                    currentRecordId++;
                } catch(JasDBStorageException e) {
                    log.error("Unable to store in index", e);
                    failures++;
                }
            }

        }
    }
	
	@Test
	public void testLongReadWriteIndex() throws Exception {
		int indexSize = 100000;
		long totalInsertTime = 0;
		KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
		BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        Map<String, String> params = new HashMap<>();
        params.put("pageSize", "512");
        index.configure(new ManualConfiguration("btree", params));
        long start = System.currentTimeMillis();
		for(int i=0; i<indexSize; i++) {
			long startInsert = System.nanoTime();
			index.insertIntoIndex(new LongKey(i).addKey(keyInfo.getKeyNameMapper(), RECORD_POINTER, new LongKey(100 + i)));
			long endInsert = System.nanoTime();
			
			totalInsertTime += (endInsert - startInsert);
		}
        assertBlocks(index.getLockManager(), index.getPersister(), index.getRootBlock(), index.getPersister().getMaxKeys(), index.getPersister().getMinKeys(), -1);
        log.info("Index: {}", index.toString());
        long end = System.currentTimeMillis();
        log.info("Total insert took: {}", (end - start));
        double averageInsertTime = ((double)totalInsertTime / indexSize);
		log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));

		long startCommit = System.nanoTime();
		index.flushIndex();
		long endCommit  = System.nanoTime();
		log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
		index.closeIndex();

		try {
			index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
            assertBlocks(index.getLockManager(), index.getPersister(), index.getRootBlock(), index.getPersister().getMaxKeys(), index.getPersister().getMinKeys(), -1);
            long totalTime = 0;
			for(int i=0; i<indexSize; i++) {
				long startSearch = System.nanoTime();
				IndexSearchResultIterator foundRecords = index.searchIndex(new EqualsCondition(new LongKey(i)), new SearchLimit());
				long endSearch = System.nanoTime();
				Assert.assertEquals("There should be one found record for: " + i, 1, foundRecords.size());
				Assert.assertEquals("Record pointer should be i + 100", Long.valueOf(i + 100), Long.valueOf(((LongKey)foundRecords.next().getKey(keyInfo.getKeyNameMapper(), RECORD_POINTER)).getKey()));
				
				totalTime += (endSearch - startSearch);
			}
			double averageTime = ((double)totalTime / indexSize);
			log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));
		} catch(Throwable e) {
            log.error("", e);
        } finally {
			index.closeIndex();
		}
	}

    @Test
    public void testStringNotInIndex() throws Exception {

    }

    @Test
    public void testIndexInsertDataKeyAndRead() throws Exception {
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new StringKeyType()), new IndexField("DATA", new DataKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

        try {
            index.insertIntoIndex(new StringKey("Amsterdam").addKey(keyInfo.getKeyNameMapper(), "DATA",
                    new DataKey("My Great piece of text".getBytes(Charset.forName("utf8")))));

            //let's try a few times to make sure the index can be requested again and again with a data resource
            assertEquals("My Great piece of text", loadDataKey(index, "Amsterdam"));
            assertEquals("My Great piece of text", loadDataKey(index, "Amsterdam"));
            assertEquals("My Great piece of text", loadDataKey(index, "Amsterdam"));
        } finally {
            index.closeIndex();
        }
    }

    private String loadDataKey(Index index, String searchText) throws JasDBStorageException, IOException {
        IndexSearchResultIterator resultIterator = index.searchIndex(new EqualsCondition(new StringKey(searchText)), Index.NO_SEARCH_LIMIT);
        assertTrue(resultIterator.hasNext());

        Key key = resultIterator.next();
        assertNotNull(key);

        DataKey dataKey = (DataKey) key.getKey(index.getKeyInfo().getKeyNameMapper(), "DATA");
        assertNotNull(dataKey);

        InputStream stream = dataKey.getStream();
        byte[] buffer = new byte[4096];
        int read = stream.read(buffer);
        assertTrue("It should have read some data, was: " + read, read > 0);

        return new String(buffer, 0, read, Charset.forName("utf8"));
    }

    @Test
    public void testFullIndex() throws Exception {
        int indexSize = 10000;
        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        for(int i=0; i<indexSize; i++) {
            long startInsert = System.nanoTime();

            index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        long startCommit = System.nanoTime();
        index.flushIndex();
        long endCommit  = System.nanoTime();
        log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
        index.closeIndex();

        try {
            index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

            int i = 0;
            Iterator<Key> keyIterator = index.getIndexIterator();
            while(keyIterator.hasNext()) {
                Key key = keyIterator.next();
                assertEquals(new LongKey(i), key);

                i++;
            }
            assertEquals(indexSize, i);
        } finally {
            index.closeIndex();
        }
    }

    @Test
    public void testFullIndexBelowPageSizeFailure() throws Exception {
        int indexSize = 100;
        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        for(int i=0; i<indexSize; i++) {
            long startInsert = System.nanoTime();

            index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        long startCommit = System.nanoTime();
        index.flushIndex();
        long endCommit  = System.nanoTime();
        log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
        index.closeIndex();

        try {
            index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

            int i = 0;
            Iterator<Key> keyIterator = index.getIndexIterator();
            while(keyIterator.hasNext()) {
                Key key = keyIterator.next();
                assertEquals(new LongKey(i), key);

                i++;
            }
            assertEquals(indexSize, i);
        } finally {
            index.closeIndex();
        }
    }


	@Test
	public void testLongReadWriteRangeSearchIndex() throws Exception {
		int indexSize = 100000;
		long totalInsertTime = 0;
		KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
		BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
		for(int i=0; i<=indexSize; i++) {
			long startInsert = System.nanoTime();
			
			index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
			long endInsert = System.nanoTime();
			
			totalInsertTime += (endInsert - startInsert);
		}
		double averageInsertTime = ((double)totalInsertTime / indexSize);
		log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
		log.debug(index.toString());
		
		long startCommit = System.nanoTime();
		index.flushIndex();
		long endCommit  = System.nanoTime();
		log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
		index.closeIndex();
		
		try {
			index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
			
			long totalTime = 0;
			for(int i=0; i<indexSize; i = i + 25) {
				log.debug("Searching from: {} till: {}", i, i+25);
				long startSearch = System.nanoTime();
				IndexSearchResultIterator results = index.searchIndex(new RangeCondition(new LongKey(i), true, new LongKey(i + 25), true), new SearchLimit());
				long endSearch = System.nanoTime();
				
				Assert.assertEquals("There should be 26 results, iteration: " + i, 26, results.size());
				
				long timeSearched = (endSearch - startSearch);
				log.debug("Search took: {}", timeSearched);
				totalTime += timeSearched;
			}
			
			double averageTime = ((double)totalTime / (indexSize / 25));
			log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));
		} finally {
			index.closeIndex();
		}
	}

    @Test
    public void testLongNotEqualsOperation() throws Exception {
        int indexSize = 10000;
        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        for(int i=0; i<=indexSize; i++) {
            long startInsert = System.nanoTime();

            index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        long startCommit = System.nanoTime();
        index.flushIndex();
        long endCommit  = System.nanoTime();
        log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
        index.closeIndex();


        try {
            index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

            long totalTime = 0;
            for(int i=0; i<indexSize; i = i + 25) {
                long startNotIn = System.nanoTime();
                assertThat(index.searchIndex(new NotEqualsCondition(new LongKey(i)), Index.NO_SEARCH_LIMIT).size(), is(indexSize));
                long endNotIn = System.nanoTime();
                totalTime += (endNotIn - startNotIn);
            }
            double averageTime = ((double)totalTime / (indexSize));
            log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));

        } finally {
            index.closeIndex();
        }
    }

    @Test
    public void testStringNotEqualsCompositeKeyOperation() throws Exception {
        String[] urlList = new String[] {"www.test.nl", "www.nu.nl", "www.tweakers.net", ""};

        int indexSize = 100000;
        long totalInsertTime = 0;
        KeyInfo keyInfo = new KeyInfoImpl(Lists.newArrayList(new IndexField("url", new StringKeyType()),
                new IndexField(RECORD_POINTER, new UUIDKeyType())), new ArrayList<IndexField>());
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        int counter = 0;
        for(int i=0; i<indexSize; i++) {
            long startInsert = System.nanoTime();

            String url = urlList[counter];
            Key compositeKey = new CompositeKey()
                    .addKey(keyNameMapper, "url", new StringKey(url))
                    .addKey(keyNameMapper, RECORD_POINTER, new UUIDKey(System.currentTimeMillis(), i));

            index.insertIntoIndex(compositeKey);
            long endInsert = System.nanoTime();

            totalInsertTime += (endInsert - startInsert);

            counter++;
            if(counter == 4) {
                counter = 0;
            }
        }
        double averageInsertTime = ((double)totalInsertTime / indexSize);
        log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
        log.debug(index.toString());

        long startCommit = System.nanoTime();
        index.flushIndex();
        long endCommit  = System.nanoTime();
        log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
        index.closeIndex();

        try {
            index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);

            int expectedSize = indexSize - (indexSize / urlList.length);
            for(String url : urlList) {
                assertThat(index.searchIndex(new NotEqualsCondition(new StringKey(url)), Index.NO_SEARCH_LIMIT).size(), is(expectedSize));
            }
        } finally {
            index.closeIndex();
        }
    }

    @Test
    public void testLongReadWriteRangeSearchIndexWithSearchLimit() throws Exception {
        int indexSize = 10000;
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
        for(int i=0; i<=indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
        }
        index.flushIndex();
        index.closeIndex();

        try {
            index = new BTreeIndex(new File(tmpDir, "indexbag_somekey.idx"), keyInfo);
            int batchSize = 50;

            for(int i=0; i<indexSize; i += batchSize) {
                IndexSearchResultIteratorCollection results = index.searchIndex(new RangeCondition(new LongKey(i), true, null, false), new SearchLimit(batchSize));
                Assert.assertEquals("Unexpected batch size result", batchSize, results.size());
                List<Key> foundKeys = results.getKeys();
                Key firstKey = foundKeys.get(0);
                Key lastKey = foundKeys.get(foundKeys.size() - 1);
                Assert.assertEquals("Unexpected key", new LongKey(i), firstKey);
                Assert.assertEquals("Unexpected key", new LongKey(((i + batchSize) - 1)), lastKey);
            }
        } finally {
            index.closeIndex();
        }
    }

    @Test
    public void testIndexUpdate() throws JasDBStorageException {
        int indexSize = 1000;
        KeyInfo keyInfo = new KeyInfoImpl(new IndexField("somekey", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_update.idx"), keyInfo);
        for(int i=0; i<indexSize; i++) {
            index.insertIntoIndex(new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i)));
        }
        log.debug("Index after insert: \n{}", index.toString());
        index.flushIndex();
        index.closeIndex();

        index = new BTreeIndex(new File(tmpDir, "indexbag_update.idx"), keyInfo);
        try {
            for(int i=0; i<indexSize; i++) {
                Key updatedKey = new LongKey(i).addKey(keyNameMapper, RECORD_POINTER, new LongKey(5000 + i));
                index.updateKey(updatedKey, updatedKey);

                IndexSearchResultIteratorCollection result = index.searchIndex(new EqualsCondition(new LongKey(i)), new SearchLimit());
                Assert.assertEquals("Expected a result", 1, result.size());
                Assert.assertEquals("Expected new updated long value", new LongKey(5000 + i), result.getKeys().get(0).getKey(keyInfo.getKeyNameMapper(),
                        RECORD_POINTER));
            }
        } finally {
            index.closeIndex();
        }
    }

	@Test
	public void testLongReadWriteRandomIndex() throws Exception {
		Map<Long, Long> longRecords = new HashMap<>();

		int indexSize = 1000;
		long totalInsertTime = 0;
		KeyInfo keyInfo = new KeyInfoImpl(new IndexField("randomlong", new LongKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
		BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_randomlong.idx"), keyInfo);
		Random rnd = new Random(System.currentTimeMillis());
		for(int i=0; i<indexSize; i++) {
			
			long randomLong = -1;
			while(randomLong < 0 || longRecords.containsKey(randomLong)) {
				randomLong = rnd.nextInt(10000);	
			}
			long startInsert = System.nanoTime();
			LongKey pointer = (LongKey) new LongKey(randomLong).addKey(keyNameMapper, RECORD_POINTER, new LongKey(100 + i));
			index.insertIntoIndex(pointer);
			longRecords.put(pointer.getKey(), ((LongKey)pointer.getKey(keyNameMapper, RECORD_POINTER)).getKey());
			long endInsert = System.nanoTime();
			
			totalInsertTime += (endInsert - startInsert);
		}
		double averageInsertTime = ((double)totalInsertTime / indexSize);
		log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));
		log.trace(index.toString());
		
		long startCommit = System.nanoTime();
		index.flushIndex();
		long endCommit  = System.nanoTime();
		log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
		index.closeIndex();
		
		try {
			index = new BTreeIndex(new File(tmpDir, "indexbag_randomlong.idx"), keyInfo);
			long totalTime = 0;
			
			for(Map.Entry<Long, Long> entry : longRecords.entrySet()) {
				long startSearch = System.nanoTime();
				IndexSearchResultIterator foundRecords = index.searchIndex(new EqualsCondition(new LongKey(entry.getKey())), new SearchLimit());
				long endSearch = System.nanoTime();

				Assert.assertEquals("There should be one found record for long: " + entry.getKey(), 1, foundRecords.size());
				Assert.assertEquals("Record pointer should be " + entry.getValue(), 
						entry.getValue(), Long.valueOf(((LongKey)foundRecords.next().getKey(keyNameMapper, RECORD_POINTER)).getKey()));
				totalTime += (endSearch - startSearch);
			}
			
			double averageTime = ((double)totalTime / indexSize);
			log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));
		} finally {
			log.trace(index.toString());
			index.closeIndex();
		}
	}
	
	@Test
	public void testUUIDReadWriteIndex() throws Exception {
		Map<UUID, Long> uuidRecords = new HashMap<>();
		int indexSize = 100000;
		long totalInsertTime = 0;
		KeyInfo keyInfo = new KeyInfoImpl(new IndexField("uuid", new UUIDKeyType()), new IndexField(RECORD_POINTER, new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
		BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_uuid.idx"), keyInfo);
		for(int i=0; i<indexSize; i++) {
			long startInsert = System.nanoTime();
			UUID uuid = UUID.randomUUID();
			Key uuidPointer = new UUIDKey(uuid).addKey(keyNameMapper, RECORD_POINTER, new LongKey(i));
			index.insertIntoIndex(uuidPointer);
			long endInsert = System.nanoTime();
			
			uuidRecords.put(uuid, Long.valueOf(i));
			totalInsertTime += (endInsert - startInsert);
		}
		double averageInsertTime = ((double)totalInsertTime / indexSize);
		log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));

		log.debug(index.toString());
		long startCommit = System.nanoTime();
		index.flushIndex();
		long endCommit  = System.nanoTime();
		log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
		index.closeIndex();
		
		
		index = new BTreeIndex(new File(tmpDir, "indexbag_uuid.idx"), keyInfo);
		try {
			long totalTime = 0;
			for(Map.Entry<UUID, Long> entry : uuidRecords.entrySet()) {
				long startSearch = System.nanoTime();
				IndexSearchResultIterator foundRecords = index.searchIndex(new EqualsCondition(new UUIDKey(entry.getKey())), new SearchLimit());
				long endSearch = System.nanoTime();
				Assert.assertEquals("There should be one found record for uuid: " + entry.getKey(), 1, foundRecords.size());
				Assert.assertEquals("Record pointer should be " + entry.getValue(), 
							Long.valueOf(entry.getValue()), Long.valueOf(((LongKey)foundRecords.next().getKey(keyNameMapper, RECORD_POINTER)).getKey()));
				totalTime += (endSearch - startSearch);
			}
			
			double averageTime = ((double)totalTime / indexSize);
			log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));
		} finally {
			log.debug(index.toString());
			index.closeIndex();
		}
	}
	
	@Test
	public void testStringReadWriteIndex() throws Exception {
		Map<String, Long> uuidRecords = new HashMap<>();
		int indexSize = 100000;
		long totalInsertTime = 0;
		KeyInfo keyInfo = new KeyInfoImpl(new IndexField("string", new StringKeyType(100)), new IndexField(RECORD_POINTER, new LongKeyType()));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
		BTreeIndex index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
		for(int i=0; i<indexSize; i++) {
			long startInsert = System.nanoTime();
			String key = "SomeKey" + i;
			Key stringPointer = new StringKey(key).addKey(keyNameMapper, RECORD_POINTER, new LongKey(i + 100));
			index.insertIntoIndex(stringPointer);
			long endInsert = System.nanoTime();
			
			uuidRecords.put(key, new Long(i + 100));
			totalInsertTime += (endInsert - startInsert);
		}
		double averageInsertTime = ((double)totalInsertTime / indexSize);
		log.info("Average insert time: {} ns. ({} ms.)", averageInsertTime, (averageInsertTime / (1000 * 1000)));

		log.debug(index.toString());
		long startCommit = System.nanoTime();
		index.flushIndex();
		long endCommit  = System.nanoTime();
		log.info("Took: {} ms. to flush index", ((endCommit - startCommit) / (1000 * 1000)));
		index.closeIndex();
		
		
		index = new BTreeIndex(new File(tmpDir, "indexbag_string.idx"), keyInfo);
		try {
			long totalTime = 0;
			for(Map.Entry<String, Long> entry : uuidRecords.entrySet()) {
				long startSearch = System.nanoTime();
				IndexSearchResultIterator foundRecords = index.searchIndex(new EqualsCondition(new StringKey(entry.getKey())), new SearchLimit());
				long endSearch = System.nanoTime();
				Assert.assertEquals("There should be one found record for uuid: " + entry.getKey(), 1, foundRecords.size());
				Assert.assertEquals("Record pointer should be " + entry.getValue(),
						Long.valueOf(entry.getValue()), getRecordValue(keyNameMapper, foundRecords.next()));
				totalTime += (endSearch - startSearch);
			}

			double averageTime = ((double)totalTime / indexSize);
			log.info("Average search time: {} ns. ({} ms.)", averageTime, (averageTime / (1000 * 1000)));
		} finally {
			index.closeIndex();
		}
	}

    @Test
    public void testIndexWithEmptyFields() throws Exception {
        String[] cities = {"", "Amsterdam", "Utrecht", "Rotterdam", "Haarlem", "Den Haag"};
        int maxAge = 120;

        KeyInfo keyInfo = new KeyInfoImpl(
                Lists.newArrayList(
                        new IndexField("age", new LongKeyType()),
                        new IndexField("city", new StringKeyType(30)),
                        new IndexField(RECORD_POINTER, new LongKeyType()))
                , new ArrayList<IndexField>());
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "index_compound.idx"), keyInfo);
        try {
            long counter = 0;
            for(String city : cities) {
                for(int age=0; age<maxAge; age++) {
                    CompositeKey key = new CompositeKey();
                            key.addKey(keyNameMapper, "age", new LongKey(age))
                            .addKey(keyNameMapper, RECORD_POINTER, new LongKey(counter));

                    if(StringUtils.stringEmpty(city)) {
                        key.addKey(keyNameMapper, "city", new StringKey(new byte[0]));
                    } else {
                        key.addKey(keyNameMapper, "city", new StringKey(city));
                    }

                    index.insertIntoIndex(key);

                    counter++;
                }
            }
        } finally {
            index.closeIndex();
        }

        index = new BTreeIndex(new File(tmpDir, "index_compound.idx"), keyInfo);
        try {
            long counter = 0;
            long total = 0;
            for(String city : cities) {
                for(int age=0; age<maxAge; age++) {
                    long start = System.currentTimeMillis();
                    IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new CompositeKey()
                            .addKey(keyNameMapper, "age", new LongKey(age))
                            .addKey(keyNameMapper, "city", new StringKey(city))), Index.NO_SEARCH_LIMIT);
                    long end = System.currentTimeMillis();
                    assertTrue(result.hasNext());
                    assertEquals(new LongKey(counter), result.next().getKey(keyNameMapper, RECORD_POINTER));
                    counter++;
                    total += (end - start);
                }
            }
            log.info("Finished querying of: {} items in: {} ms.", counter, total);

            IndexSearchResultIterator result = index.searchIndex(new RangeCondition(new LongKey(10), true, new LongKey(20), false), Index.NO_SEARCH_LIMIT);
            counter = 0;
            while(result.hasNext()) {
                Key key = result.next();
                assertNotNull(key);
                counter++;
            }
            assertEquals(10 * cities.length, counter);
        } finally {
            index.closeIndex();
        }

    }

    @Test
    public void testCompoundKeyIndex() throws Exception {
        String[] cities = {"Amsterdam", "Utrecht", "Rotterdam", "Haarlem", "Den Haag"};
        int maxAge = 120;

        KeyInfo keyInfo = new KeyInfoImpl(
                Lists.newArrayList(new IndexField("age", new LongKeyType()), new IndexField("city", new StringKeyType(30))),
                Lists.newArrayList(new IndexField(RECORD_POINTER, new LongKeyType())));
        KeyNameMapper keyNameMapper = keyInfo.getKeyNameMapper();
        BTreeIndex index = new BTreeIndex(new File(tmpDir, "index_compound.idx"), keyInfo);
        try {
            long counter = 0;
            for(String city : cities) {
                for(int age=0; age<maxAge; age++) {
                    index.insertIntoIndex(new CompositeKey()
                            .addKey(keyNameMapper, "age", new LongKey(age))
                            .addKey(keyNameMapper, "city", new StringKey(city))
                            .addKey(keyNameMapper, RECORD_POINTER, new LongKey(counter)));
                    counter++;
                }
            }
        } finally {
            index.closeIndex();
        }

        index = new BTreeIndex(new File(tmpDir, "index_compound.idx"), keyInfo);
        try {
            long counter = 0;
            long total = 0;
            for(String city : cities) {
                for(int age=0; age<maxAge; age++) {
                    long start = System.currentTimeMillis();
                    IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new CompositeKey()
                        .addKey(keyNameMapper, "age", new LongKey(age))
                        .addKey(keyNameMapper, "city", new StringKey(city))), Index.NO_SEARCH_LIMIT);
                    long end = System.currentTimeMillis();
                    assertTrue(result.hasNext());
                    assertEquals(new LongKey(counter), result.next().getKey(keyNameMapper, RECORD_POINTER));
                    counter++;
                    total += (end - start);
                }
            }
            log.info("Finished querying of: {} items in: {} ms.", counter, total);

            IndexSearchResultIterator result = index.searchIndex(new RangeCondition(new LongKey(10), true, new LongKey(20), false), Index.NO_SEARCH_LIMIT);
            counter = 0;
            while(result.hasNext()) {
                Key key = result.next();
                assertNotNull(key);
                counter++;
            }
            assertEquals(10 * cities.length, counter);
        } finally {
            index.closeIndex();
        }
    }

    private Long getRecordValue(KeyNameMapper keyNameMapper, Key key) {
		return Long.valueOf(((LongKey)key.getKey(keyNameMapper, RECORD_POINTER)).getKey());
	}
}
