package com.oberasoftware.jasdb.engine;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.btreeplus.BTreeIndex;
import com.oberasoftware.jasdb.core.index.keys.DataKey;
import com.oberasoftware.jasdb.core.index.keys.UUIDKey;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.types.DataKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.UUIDKeyType;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.writer.transactional.FSWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Renze de Vries
 */
public class ThroughputTest {
    private static final Logger LOG = LoggerFactory.getLogger(ThroughputTest.class);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String storageLocation;

    @Before
    public void setup() throws IOException {
        storageLocation = temporaryFolder.newFolder().toString();
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testFSWriterThroughput() throws Exception {
        File dataFile = new File(storageLocation, "throughput.data");
        if(dataFile.exists()) {
            dataFile.delete();
        }

        FSWriter recordWriter = new FSWriter(dataFile);
        recordWriter.openWriter();
        FSWriterAction action = new FSWriterAction(recordWriter);
        try {
            assertThat(runTest(20, 10000, action) < 30, is(true));
        } finally {
            recordWriter.closeWriter();
            dataFile.deleteOnExit();
        }
    }

    @Test
    public void testStringKeyWriteThroughput() throws Exception {
        KeyInfoImpl keyInfo = new KeyInfoImpl(new SimpleIndexField("id", new UUIDKeyType()), new SimpleIndexField("data", new DataKeyType()));
        File dataFile = new File(storageLocation, "throughput.data");
        if(dataFile.exists()) {
            dataFile.delete();
        }

        BTreeIndex index = new BTreeIndex(dataFile, keyInfo);
        index.openIndex();
        BTreeIndexWrite action = new BTreeIndexWrite(index);
        try {
            assertThat(runTest(20, 10000, action) < 30, is(true));
        } finally {
            index.close();
            dataFile.deleteOnExit();
        }
    }

    private long runTest(int threads, int nrRecords, Action action) throws InterruptedException {
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

        return seconds;
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
                    assertThat(action.execute(key, randomString), is(true));
                }
                long end = System.currentTimeMillis();
                LOG.debug("Thread: {} finished persisting in: {} ms.", threadId, (end - start));
            } catch(InterruptedException e) {
                LOG.error("", e);
            }
        }
    }

    private interface Action {
        boolean execute(UUIDKey key, String data);
    }

    private class FSWriterAction implements Action {
        private FSWriter recordWriter;

        public FSWriterAction(FSWriter recordWriter) {
            this.recordWriter = recordWriter;
        }

        @Override
        public boolean execute(UUIDKey key, String data) {
            try {
                recordWriter.writeRecord(data, null);
                return true;
            } catch(JasDBStorageException e) {
                LOG.error("", e);
                return false;
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
        public boolean execute(UUIDKey key, String data) {
            key.addKey(mapper, "data", new DataKey(data.getBytes()));
            try {
                index.insertIntoIndex(key);
                return true;
            } catch(JasDBStorageException e) {
                LOG.error("", e);
                return false;
            }
        }
    }
}
