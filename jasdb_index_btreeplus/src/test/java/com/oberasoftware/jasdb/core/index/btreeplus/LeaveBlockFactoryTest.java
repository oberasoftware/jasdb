package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.storage.DataBlockFactory;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.core.storage.DataBlockFactoryImpl;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.BtreePlusBlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.LeaveBlockFactory;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.core.index.keys.LongKey;
import com.oberasoftware.jasdb.core.index.keys.StringKey;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.core.index.keys.keyinfo.KeyInfoImpl;
import com.oberasoftware.jasdb.api.index.keys.KeyNameMapper;
import com.oberasoftware.jasdb.core.index.keys.types.LongKeyType;
import com.oberasoftware.jasdb.core.index.keys.types.StringKeyType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class LeaveBlockFactoryTest {
    private static final Logger LOG = LoggerFactory.getLogger(LeaveBlockFactoryTest.class);

    private static final int DEFAULT_BLOCK_SIZE = 8192;

    private DataBlockFactory dataBlockFactory;
    private FileChannel channel;
    private RandomAccessFile ram;

    private BtreePlusBlockPersister persister;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws IOException, JasDBException {
        File tempFile = temporaryFolder.newFile();
        ram = new RandomAccessFile(tempFile, "rw");
        channel = ram.getChannel();
        dataBlockFactory = new DataBlockFactoryImpl(tempFile, channel, DEFAULT_BLOCK_SIZE);
        dataBlockFactory.open();

        persister = mock(BtreePlusBlockPersister.class);
        when(persister.getDataBlockFactory()).thenReturn(dataBlockFactory);

    }

    @After
    public void tearDown() throws JasDBException, IOException {
        dataBlockFactory.close();
        channel.close();
        ram.close();
    }

    @Test
    public void testStoreAndLoadLeave() throws JasDBException {
        int maxKeys = 512;
        int nrKeys = 500;

        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("field1", new StringKeyType(200)), new SimpleIndexField("POINTER", new LongKeyType()));
        when(persister.getMaxKeys()).thenReturn(maxKeys);
        when(persister.getKeyInfo()).thenReturn(keyInfo);

        LeaveBlockFactory leaveBlockFactory = new LeaveBlockFactory(persister);
        LOG.info("Starting writing keys");
        long leaveBlockPointer = generateLeaveBlock(leaveBlockFactory, keyInfo.getKeyNameMapper(), nrKeys);
        LOG.info("Finished writing keys");

        LeaveBlockImpl leaveBlock = (LeaveBlockImpl) leaveBlockFactory.loadBlock(dataBlockFactory.loadBlock(leaveBlockPointer));
        List<Key> keys = leaveBlock.getValues();
        assertEquals(nrKeys, keys.size());

        for(int keyNr=0; keyNr<nrKeys; keyNr++) {
            assertTrue(leaveBlock.contains(new StringKey("fieldKey" + keyNr)));

            Key key = leaveBlock.getKey(new StringKey("fieldKey" + keyNr));

            assertEquals(new StringKey("fieldKey" + keyNr), key);
            assertEquals(new LongKey(keyNr), key.getKey(0));
        }
    }

    private long generateLeaveBlock(LeaveBlockFactory leaveBlockFactory, KeyNameMapper nameMapper, int keys) throws JasDBException {
        LeaveBlockImpl leaveBlock = leaveBlockFactory.createBlock(555, dataBlockFactory.getBlockWithSpace(false));
        long position = leaveBlock.getPosition();

        for(int keyNr=0; keyNr < keys; keyNr++) {
            Key key = new StringKey("fieldKey" + keyNr).addKey(nameMapper, "POINTER", new LongKey(keyNr));
            leaveBlock.insertKey(key);
        }

        leaveBlockFactory.persistBlock(leaveBlock);
        leaveBlock.close();

        return position;
    }
}
