package nl.renarj.storage;

import com.oberasoftware.jasdb.service.local.LocalDBSession;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.jasdb.api.DBSession;
import nl.renarj.jasdb.api.SimpleEntity;
import nl.renarj.jasdb.api.model.EntityBag;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.btreeplus.BTreeIndex;
import nl.renarj.jasdb.index.keys.impl.DataKey;
import nl.renarj.jasdb.index.keys.impl.UUIDKey;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfoImpl;
import nl.renarj.jasdb.index.keys.keyinfo.KeyNameMapper;
import nl.renarj.jasdb.index.keys.types.DataKeyType;
import nl.renarj.jasdb.index.keys.types.UUIDKeyType;
import nl.renarj.jasdb.index.search.IndexField;
import nl.renarj.jasdb.storage.transactional.FSWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Renze de Vries
 */
public class ThroughputTest extends DBBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(ThroughputTest.class);

    @Before
    public void setup() {
        cleanData();
    }

    @After
    public void after() throws Exception {
        super.tearDown();
        cleanData();
    }

    @Test
    @Ignore
    public void testFSWriterThroughput() throws Exception {
        File dataFile = new File(DBBaseTest.tmpDir, "throughput.data");
        if(dataFile.exists()) {
            dataFile.delete();
        }

        FSWriter recordWriter = new FSWriter(dataFile);
        recordWriter.openWriter();
        FSWriterAction action = new FSWriterAction(recordWriter);
        try {
            runTest(20, 100000, action);
        } finally {
            recordWriter.closeWriter();
            dataFile.deleteOnExit();
        }
    }

    @Test
    @Ignore
    public void testStringKeyWriteThroughput() throws Exception {
        KeyInfoImpl keyInfo = new KeyInfoImpl(new IndexField("id", new UUIDKeyType()), new IndexField("data", new DataKeyType()));
        File dataFile = new File(DBBaseTest.tmpDir, "throughput.data");
        if(dataFile.exists()) {
            dataFile.delete();
        }

        BTreeIndex index = new BTreeIndex(dataFile, keyInfo);
        index.openIndex();
        BTreeIndexWrite action = new BTreeIndexWrite(index);
        try {
            runTest(20, 100000, action);
        } finally {
            index.close();
            dataFile.deleteOnExit();
        }
    }

    @Test
    @Ignore
    public void testSessionEntityInsertThroughput() throws Exception {
        DBSession session = new LocalDBSession();

        EntityInsertAction entityInsertAction = new EntityInsertAction(session);
        runTest(20, 100000, entityInsertAction);

        StatisticsMonitor.logStats(TimeUnit.NANOSECONDS);
    }

//    @Test
//    @Ignore
//    public void testMongoInsertThroughput() throws Exception {
//        MongoClient mongoClient = new MongoClient();
//        DB db = mongoClient.getDB("mydb");
//        MongoInsertAction mongoInsertAction = new MongoInsertAction(db.getCollection("inserts"));
//        runTest(20, 100000, mongoInsertAction);
//    }

    private void runTest(int threads, int nrRecords, Action action) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(1);
        for(int i=0; i<threads; i++) {
            executorService.submit(new WriteThread(nrRecords, action, latch));
        }
        long start = System.currentTimeMillis();
        latch.countDown();
        executorService.shutdown();
        executorService.awaitTermination(240, TimeUnit.SECONDS);
        long end = System.currentTimeMillis();
        long seconds = TimeUnit.SECONDS.convert((end - start), TimeUnit.MILLISECONDS);
        double perSecond = (double)(nrRecords * threads) / (double)seconds;
        LOG.info("Persisting done in: {} secs", seconds);
        LOG.info("Throughput: {} records/sec", perSecond);
    }

    private class WriteThread implements Runnable {
        private SecureRandom random = new SecureRandom();
        private final int nrRecords;
        private final Action action;
        private CountDownLatch latch;

        public WriteThread(int nrRecords, Action action, CountDownLatch latch) {
            this.nrRecords = nrRecords;
            this.action = action;
            this.latch = latch;
        }

        @Test
        public void run() {
            try {
                long threadId = Thread.currentThread().getId();
                latch.await();

                long start = System.currentTimeMillis();
                for(int i=0; i<nrRecords; i++) {
                    String randomString = new BigInteger(130, random).toString(32);
                    UUIDKey key = new UUIDKey(threadId, i);
                    action.execute(key, randomString);
                }
                long end = System.currentTimeMillis();
                LOG.debug("Thread: {} finished persisting in: {} ms.", threadId, (end - start));
            } catch(InterruptedException e) {
                LOG.error("", e);
            }
        }
    }

    private interface Action {
        void execute(UUIDKey key, String data);
    }

    private class EntityInsertAction implements Action {
        private DBSession session;
        private EntityBag entityBag;

        public EntityInsertAction(DBSession dbSession) throws JasDBStorageException {
            this.session = dbSession;
            this.entityBag = session.createOrGetBag("databag");
        }

        @Override
        public void execute(UUIDKey key, String data) {
            SimpleEntity entity = new SimpleEntity();
            entity.addProperty("data", data);

            try {
                entityBag.addEntity(entity);
            } catch(JasDBStorageException e) {
                LOG.error("", e);
            }
        }
    }

//    private class MongoInsertAction implements Action {
//        private DBCollection dbCollection;
//
//        public MongoInsertAction(DBCollection dbCollection) {
//            this.dbCollection = dbCollection;
//        }
//
//        @Override
//        public void execute(UUIDKey key, String data) {
//            BasicDBObject doc = new BasicDBObject("data", data);
//            dbCollection.insert(doc, WriteConcern.UNACKNOWLEDGED);
//        }
//    }

    private class FSWriterAction implements Action {
        private FSWriter recordWriter;

        public FSWriterAction(FSWriter recordWriter) {
            this.recordWriter = recordWriter;
        }

        @Override
        public void execute(UUIDKey key, String data) {
            try {
                recordWriter.writeRecord(data, null);
            } catch(JasDBStorageException e) {
                LOG.error("", e);
            }
        }
    }

    private class BTreeIndexWrite implements Action {
        private BTreeIndex index;
        private KeyNameMapper mapper;

        public BTreeIndexWrite(BTreeIndex index) {
            this.index = index;
            this.mapper = index.getKeyInfo().getKeyNameMapper();
        }

        @Override
        public void execute(UUIDKey key, String data) {
            key.addKey(mapper, "data", new DataKey(data.getBytes()));
            try {
                index.insertIntoIndex(key);
            } catch(JasDBStorageException e) {
                LOG.error("", e);
            }
        }
    }
}
