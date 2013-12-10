package nl.renarj.storage;

import junit.framework.Assert;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.LocalDBSession;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.SimpleKernel;
import nl.renarj.jasdb.core.exceptions.JasDBException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.utils.HomeLocatorUtil;
import nl.renarj.jasdb.index.keys.types.StringKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;

public class ConcurrencyTest extends DBBaseTest {
	private static final Logger log = LoggerFactory.getLogger(ConcurrencyTest.class);

    private static final Random rnd = new Random(System.currentTimeMillis());
	private static final int NUMBER_ENTITIES = 10000;
	private static final int NUMBER_THREADS = 20;

    private static final String TESTBAG = "testbag";
    private static final String NEW_PROPERTY = "newProperty";
    private static final String MY_NEW_VALUE = "MyNewValue";

    private List<String> createdIds = new ArrayList<>();
	
	@Before
	public void setUp() {
        System.setProperty(HomeLocatorUtil.JASDB_HOME, DBBaseTest.tmpDir.toString());

        cleanData();
	}
	
	@After
	public void tearDown() throws JasDBException {
        super.tearDown();

        cleanData();
	}

    private List<String> createData(int amount) throws JasDBException {
        return createData(amount, true);
    }

	private List<String> createData(int amount, boolean shutdownKernel) throws JasDBException {
		DBSession pojoDb = new LocalDBSession();
		EntityBag bag = pojoDb.createOrGetBag(TESTBAG);
        bag.ensureIndex(new IndexField("invertedkey", new StringKeyType(20)), false);

		try {
			log.info("Starting data preperations");
            List<String> generatedIds = new ArrayList<>(amount);
			for(int i=0; i<amount; i++) {
				SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
				entity.addProperty("someProperty" + i, i);
				entity.addProperty("doubleId", entity.getInternalId());

                int key = rnd.nextInt(10000);
                entity.addProperty("invertedkey", "key" + key);
				bag.addEntity(entity);
				
				generatedIds.add(entity.getInternalId());
			}
			log.info("Finished preparing data");
            return generatedIds;
		} finally {
			if(shutdownKernel) {
                SimpleKernel.shutdown();
            }
		}
	}

    private Map<Thread, ReadThread> createReaderThreads(int amount, boolean start) throws JasDBStorageException {
        Map<Thread, ReadThread> readers = new HashMap<>();
        for(int i=0; i<amount; i++) {
            ReadThread reader = new ReadThread("reader" + i, new ArrayList<>(createdIds));
            Thread readerThread = new Thread(reader, "Reader" + i);
            if(start) {
                readerThread.start();
            }

            readers.put(readerThread, reader);
        }

        return readers;
    }

    private Map<Thread, WriterThread> createWriterThreads(int amount, int storeAmount) throws JasDBStorageException {
        Map<Thread, WriterThread> writers = new HashMap<>();
        for(int i=0; i<amount; i++) {
            WriterThread writer = new WriterThread("writer" + i, storeAmount);
            Thread writerThread = new Thread(writer, "Writer" + i);

            writers.put(writerThread, writer);
        }

        return writers;
    }

    private Map<Thread, UpdateThread> createUpdateThreads(int amount) throws JasDBStorageException {
        Map<Thread, UpdateThread> updaters = new HashMap<>();
        for(int i=0; i<amount; i++) {
            UpdateThread update = new UpdateThread("update" + i, createdIds);
            Thread updateThread = new Thread(update, "Update" + i);

            updaters.put(updateThread, update);
        }

        return updaters;
    }

    private Map<Thread, RemoveThread> createRemoveThreads(int amount) throws JasDBException {
        Map<Thread, RemoveThread> removers = new HashMap<>();
        for(int i=0; i<amount; i++) {
            RemoveThread remover = new RemoveThread("remove" + i, createData(NUMBER_ENTITIES, false));
            Thread removeThread = new Thread(remover, "Remove" + i);

            removers.put(removeThread, remover);
        }

        return removers;
    }

    private void startThreads(List<Thread> threads) {
        for(Thread thread : threads) {
            thread.start();
        }
    }

    @Test
	public void testMultipleReadThreads() throws Exception {
		createdIds = createData(NUMBER_ENTITIES);
		StatisticsMonitor.logStats(TimeUnit.NANOSECONDS);
		
		log.info("Starting index warmup");
		loadRecords(createdIds);
		log.info("Finished warming index");

		Map<Thread, ReadThread> readers = createReaderThreads(NUMBER_THREADS, true);

		/* Let's wait till all threads are finished */
        assertReadFailures(readers);

		SimpleKernel.shutdown();
	}

	@Test
	public void testMultipleReadWriteThreads() throws Exception {
        createdIds = createData(NUMBER_ENTITIES);
		
		log.info("Starting index warmup");
		loadRecords(createdIds);
		log.info("Finished warming index");

		int nrReaderThreads = 10;
		int nrWriterThreads = 5;
		int writerCreateItems = 10000;
		
		List<Thread> threads = new ArrayList<>();
		Map<Thread, ReadThread> readers = createReaderThreads(nrReaderThreads, false);
		Map<Thread, WriterThread> writers = createWriterThreads(nrWriterThreads, writerCreateItems);

        threads.addAll(readers.keySet());
        threads.addAll(writers.keySet());
        startThreads(threads);

		/* Let's wait till all threads are finished */
        assertReadFailures(readers);
        assertWriteFailures(writers);

		SimpleKernel.shutdown();
	}

    @Test
    public void testMultipleReadWriteUpdateThreads() throws Exception {
        createdIds = createData(NUMBER_ENTITIES);

        log.info("Starting index warmup");
        loadRecords(createdIds);
        log.info("Finished warming index");

        int nrReaderThreads = 10;
        int nrUpdateThreads = 5;
        int nrWriterThreads = 5;
        int writerCreateItems = 10000;

        List<Thread> threads = new ArrayList<>();
        Map<Thread, ReadThread> readers = createReaderThreads(nrReaderThreads, false);
        Map<Thread, WriterThread> writers = createWriterThreads(nrWriterThreads, writerCreateItems);
        Map<Thread, UpdateThread> updaters = createUpdateThreads(nrUpdateThreads);

        threads.addAll(readers.keySet());
        threads.addAll(updaters.keySet());
        threads.addAll(writers.keySet());

//        StatisticsMonitor.enableStatistics();
        startThreads(threads);

        /* Let's wait till all threads are finished */
        try {
            assertReadFailures(readers);
            assertWriteFailures(writers);
            assertUpdateFailures(updaters);

//            StatisticsMonitor.logStats(TimeUnit.NANOSECONDS);
        } finally {
            for(Map.Entry<Thread, ReadThread> readerEntry : readers.entrySet()) {
                readerEntry.getValue().stop();
                readerEntry.getKey().join();
            }
            for(Map.Entry<Thread, WriterThread> writerEntry : writers.entrySet()) {
                writerEntry.getValue().stop();
                writerEntry.getKey().join();
            }
            for(Map.Entry<Thread, UpdateThread> updaterEntry : updaters.entrySet()) {
                updaterEntry.getValue().stop();
                updaterEntry.getKey().join();
            }

        }

        SimpleKernel.shutdown();

        SimpleKernel.initializeKernel();
        try {
            loadRecords(createdIds);
        } finally {
            SimpleKernel.shutdown();
        }
    }

    @Test
    public void testMultipleReadWriteUpdateRemoveThreads() throws Exception {
        createdIds = createData(NUMBER_ENTITIES);

        int nrReaderThreads = 10;
        int nrUpdateThreads = 4;
        int nrRemoveThreads = 4;
        int nrWriterThreads = 4;
        int writerCreateItems = 10000;

        List<Thread> threads = new ArrayList<>();
        Map<Thread, ReadThread> readers = createReaderThreads(nrReaderThreads, false);
        Map<Thread, WriterThread> writers = createWriterThreads(nrWriterThreads, writerCreateItems);
        Map<Thread, UpdateThread> updaters = createUpdateThreads(nrUpdateThreads);
        Map<Thread, RemoveThread> removers = createRemoveThreads(nrRemoveThreads);

        log.info("Starting index warmup");
        loadRecords(createdIds);
        log.info("Finished warming index");

        threads.addAll(readers.keySet());
        threads.addAll(updaters.keySet());
        threads.addAll(writers.keySet());
        threads.addAll(removers.keySet());
        startThreads(threads);

        try {
            /* Let's wait till all threads are finished */
            assertReadFailures(readers);
            assertWriteFailures(writers);
            assertUpdateFailures(updaters);
            assertRemoveFailures(removers);
        } finally {
            for(Map.Entry<Thread, ReadThread> readerEntry : readers.entrySet()) {
                readerEntry.getValue().stop();
                readerEntry.getKey().join();
            }
            for(Map.Entry<Thread, UpdateThread> updaterEntry : updaters.entrySet()) {
                updaterEntry.getValue().stop();
                updaterEntry.getKey().join();
            }
            for(Map.Entry<Thread, WriterThread> writerEntry : writers.entrySet()) {
                writerEntry.getValue().stop();
                writerEntry.getKey().join();
            }
            for(Map.Entry<Thread, RemoveThread> removerEntry : removers.entrySet()) {
                removerEntry.getValue().stop();
                removerEntry.getKey().join();
            }
        }


        SimpleKernel.shutdown();
    }

    private void assertWriteFailures(Map<Thread, WriterThread> writers) throws InterruptedException, JasDBStorageException {
        for(Map.Entry<Thread, WriterThread> writerEntry : writers.entrySet()) {
            writerEntry.getKey().join();
            WriterThread writerThread = writerEntry.getValue();

            log.info("Write times for: {} average: {} ns. ({} ms.) first: {} last: {}", new Object[] {
                    writerThread.getWriterId(), writerThread.getAverageWrite(), (double)writerThread.getAverageWrite() / (double)(1000 * 1000),
                    writerThread.getFirstWrite(), writerThread.getLastWrite()});

            List<String> createdIds = writerThread.getCreatedIds();
            loadRecords(createdIds);
        }

    }

    private void assertReadFailures(Map<Thread, ReadThread> readers) throws InterruptedException {
        for(Map.Entry<Thread, ReadThread> readerEntry : readers.entrySet()) {
            readerEntry.getKey().join();
            ReadThread readThread = readerEntry.getValue();
            log.info("Reader: {} has {} success and {} failures", new Object[] {readThread.readerId,
                    readThread.success, readThread.failures});
            log.info("Read times for: {} average: {} ns. ({} ms.) first: {} last: {}", new Object[] {
                    readThread.readerId, readThread.getAverageRead(), (double)readThread.getAverageRead() / (double)(1000 * 1000),
                    readThread.getFirstRead(), readThread.getLastRead()});
            Assert.assertEquals("There should be no failures", 0, readerEntry.getValue().failures);
            Assert.assertEquals("All should be found", createdIds.size(), readerEntry.getValue().success);
        }
    }

    private void assertUpdateFailures(Map<Thread, UpdateThread> updaters) throws InterruptedException, JasDBStorageException {
        for(Map.Entry<Thread, UpdateThread> updateEntry : updaters.entrySet()) {
            updateEntry.getKey().join();
            UpdateThread updateThread = updateEntry.getValue();

            log.info("Updater: {} has {} success and {} failures", new Object[] {updateThread.updateId,
                    updateThread.success, updateThread.failures});
            log.info("Update times for: {} average: {} ns. ({} ms.)", new Object[] {
                    updateThread.updateId, updateThread.getAverageUpdate(),  (double)updateThread.getAverageUpdate() / (double)(1000 * 1000) });

            Assert.assertEquals("There should be no failures", 0, updateThread.failures);
            Assert.assertEquals("There should be full success", NUMBER_ENTITIES, updateThread.success);
        }

        EntityBag bag = new LocalDBSession().createOrGetBag(TESTBAG);
        for(String updateId : createdIds) {
            SimpleEntity entity = bag.getEntity(updateId);
            Assert.assertTrue(entity.hasProperty(NEW_PROPERTY));
            Assert.assertEquals("Unexpected value after update", MY_NEW_VALUE, entity.getProperty(NEW_PROPERTY).getFirstValueObject());
        }
    }

    private void assertRemoveFailures(Map<Thread, RemoveThread> removers) throws InterruptedException, JasDBStorageException {
        for(Map.Entry<Thread, RemoveThread> removeEntry : removers.entrySet()) {
            removeEntry.getKey().join();
            RemoveThread removeThread = removeEntry.getValue();

            log.info("Remover: {} has {} success and {} failures", new Object[] {removeThread.threadId,
                    removeThread.success, removeThread.failures});
            log.info("Remove times for: {} average: {} ns. ({} ms.)", new Object[] {
                    removeThread.threadId, removeThread.getAverageRemove(),  (double)removeThread.getAverageRemove() / (double)(1000 * 1000) });


            Assert.assertEquals("There should be no failures", 0, removeThread.failures);

            EntityBag bag = new LocalDBSession().createOrGetBag(TESTBAG);
            for(String id : removeThread.removeIds) {
                Assert.assertNull("There should no longer be an entity", bag.getEntity(id));
            }
        }

    }
	
	private void loadRecords(List<String> ids) throws JasDBStorageException {
        DBSession session = new LocalDBSession();
        EntityBag entityBag = session.getBag(TESTBAG);

		for(String id : ids) {
            SimpleEntity entity = entityBag.getEntity(id);

			assertNotNull("There should be an entity present for id: " + id, entity);
		}
	}

    private static abstract class TestThread implements Runnable {
        private boolean running = true;

        public void stop() {
            running = false;
        }

        public boolean shouldContinue() {
            return running;
        }
    }

    private static class RemoveThread extends TestThread {
        private List<String> removeIds = new ArrayList<>();
        private String threadId;
        private EntityBag bag;

        private long totalTimeRemove;

        private int success = 0;
        private int failures = 0;

        public RemoveThread(String threadId, List<String> removeIds) throws JasDBStorageException {
            this.removeIds = removeIds;
            this.threadId = threadId;
            this.bag = new LocalDBSession().createOrGetBag(TESTBAG);
        }

        public long getAverageRemove() {
            return totalTimeRemove / removeIds.size();
        }

        public void run() {
            for(String removeId : removeIds) {
                if(shouldContinue()) {
                    try {
                        SimpleEntity entity = bag.getEntity(removeId);
                        if(entity != null) {
                            long start = System.nanoTime();
                            bag.removeEntity(entity);
                            long end = System.nanoTime();
                            totalTimeRemove += (end - start);

                            success++;
                            continue;
                        }
                        failures++;
                    } catch(JasDBStorageException e) {
                        log.error("Unable to remove entity", e);
                        failures++;
                        break;
                    } catch(Throwable e) {
                        log.error("Unable to remove entity, unknown error", e);
                        failures++;
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    private static class UpdateThread extends TestThread {
        private List<String> updateIds;
        private String updateId;
        private EntityBag bag;

        private long totalTimeUpdate = 0;

        private int failures = 0;
        private int success = 0;

        private UpdateThread(String updateId, List<String> updateIds) throws JasDBStorageException {
            this.updateIds = updateIds;
            this.updateId = updateId;
            this.bag = new LocalDBSession().createOrGetBag(TESTBAG);
        }

        public long getAverageUpdate() {
            return totalTimeUpdate / updateIds.size();
        }

        public void run() {
            for(String id : updateIds) {
                if(shouldContinue()) {
                    try {
                        SimpleEntity entity = bag.getEntity(id);
                        if(entity != null) {
                            entity.addProperty(NEW_PROPERTY, MY_NEW_VALUE);
                            long start = System.nanoTime();
                            bag.updateEntity(entity);
                            long end = System.nanoTime();
                            totalTimeUpdate += (end - start);

                            success++;
                            continue;
                        }

                        failures++;
                    } catch(Throwable e) {
                        log.error("Error in update thread", e);
                        failures++;
                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }
	
	private static class ReadThread extends TestThread {
		private String readerId;
		private List<String> internalIds;
		private EntityBag bag;
		
		private int failures = 0;
		private int success = 0;
		
		private long totalTimeRead;
		
		private long firstRead = 0;
		private long lastRead = 0;
		
		private ReadThread(String readerId, List<String> internalIds) throws JasDBStorageException {
			this.internalIds = internalIds;
			
			DBSession pojoDb = new LocalDBSession();
			this.bag = pojoDb.createOrGetBag(TESTBAG);
			this.readerId = readerId;
		}
		
		public long getAverageRead() {
			return totalTimeRead / NUMBER_ENTITIES;
		}
		
		public long getLastRead() {
			return this.lastRead;
		}
		
		public long getFirstRead() {
			return this.firstRead;
		}
		
		public void run() {
			log.debug("Thread: {} has started", readerId);
			for(int i=0; i<internalIds.size(); i++) {
				String internalId = internalIds.get(rnd.nextInt(internalIds.size()));
				try {
					long startRecord = System.nanoTime();
					SimpleEntity entity = this.bag.getEntity(internalId);
					long endRecord = System.nanoTime();
					long timePassed = (endRecord - startRecord);
					
					log.trace("Read: {} took: {}", i, timePassed);
					totalTimeRead += timePassed;
					if(i==0) {
						firstRead = timePassed;
					} 
					lastRead = timePassed;
					
					if(entity != null && entity.getInternalId().equals(internalId)) {
						success++;
					} else {
                        log.error("Invalid entity: {} internalId expected: {} but was: {}", new Object[] {entity, internalId, entity != null ? entity.getInternalId() : null});

						failures++;
					}
				} catch(Throwable e) {
					log.error("Error in reading thread: " + readerId, e);
					failures++;
                    break;
				}
			}
			log.debug("Thread: {} has finished", readerId);
		}
	}
	
	private static class WriterThread extends TestThread {
		private String writerId;
		private EntityBag bag;
		private int createItems;
		
		private List<String> createdIds = new ArrayList<>();
		private long totalTimeWrite;
		
		
		private long firstWrite = 0;
		private long lastWrite = 0;
		
		private WriterThread(String writerId, int createItems) throws JasDBStorageException {
			DBSession pojoDb = new LocalDBSession();
			this.bag = pojoDb.createOrGetBag(TESTBAG);
			this.writerId = writerId;
			this.createItems = createItems;
		}
		
		public String getWriterId() {
			return this.writerId;
		}
		
		public List<String> getCreatedIds() {
			return this.createdIds;
		}
		
		public long getAverageWrite() {
			return totalTimeWrite / createItems;
		}
		
		public long getLastWrite() {
			return this.lastWrite;
		}
		
		public long getFirstWrite() {
			return this.firstWrite;
		}
		
		public void run() {
			log.debug("Writer Thread: {} has started", writerId);
			for(int i=0; i<createItems; i++) {
				try {
					SimpleEntity entity = new SimpleEntity(UUID.randomUUID().toString());
					entity.addProperty("someProperty" + i, i);
					entity.addProperty("doubleId", entity.getInternalId());

                    int key = rnd.nextInt(10000);
                    entity.addProperty("invertedkey", "key" + key);
					
					long startRecord = System.nanoTime();
					bag.addEntity(entity);
					long endRecord = System.nanoTime();
					long timePassed = (endRecord - startRecord);
					
					if(i==0) {
						firstWrite = (endRecord - startRecord);
					} 
					lastWrite = timePassed;
					totalTimeWrite += timePassed;
					
					log.trace("Write: {} took: {}", entity.getInternalId(), timePassed);
					
					createdIds.add(entity.getInternalId());
				} catch(JasDBStorageException e) {
					log.error("Error in writer thread: " + writerId, e);
                    break;
				} catch(Throwable e) {
                    log.error("Unknown error in writer thread, finished: " + createdIds.size(), e);
                    break;
                }
			}
			log.debug("Writer Thread: {} has finished", writerId);
		}
	}

}
