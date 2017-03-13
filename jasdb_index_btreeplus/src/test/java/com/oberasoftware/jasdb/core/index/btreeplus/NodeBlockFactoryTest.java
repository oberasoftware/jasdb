package com.oberasoftware.jasdb.core.index.btreeplus;

import com.oberasoftware.jasdb.api.exceptions.JasDBException;
import com.oberasoftware.jasdb.api.storage.DataBlockFactory;
import com.oberasoftware.jasdb.core.index.query.SimpleIndexField;
import com.oberasoftware.jasdb.core.storage.DataBlockFactoryImpl;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.BtreePlusBlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.persistence.NodeBlockFactory;
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Renze de Vries
 */
public class NodeBlockFactoryTest {
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
    public void testStoreAndLoad() throws JasDBException {
        int nrKeys = 500;
        int maxKeys = 500;
        KeyInfo keyInfo = new KeyInfoImpl(new SimpleIndexField("field1", new StringKeyType(200)), new SimpleIndexField("POINTER", new LongKeyType()));
        when(persister.getMaxKeys()).thenReturn(maxKeys);
        when(persister.getKeyInfo()).thenReturn(keyInfo);

        NodeBlockFactory nodeBlockFactory = new NodeBlockFactory(persister);
        long position = generateAndInsert(nodeBlockFactory, keyInfo.getKeyNameMapper(), nrKeys);

        TreeBlock loadedBlock = nodeBlockFactory.loadBlock(dataBlockFactory.loadBlock(position));
        assertEquals(444, loadedBlock.getParentPointer());
        for(int i=0; i<nrKeys; i++) {
            Key expectedKey = new StringKey("fieldKey" + i);
            assertTrue(loadedBlock.getNodes().contains(expectedKey));
            TreeNode node = loadedBlock.getNodes().get(expectedKey);

            assertEquals(i + 10000, node.getLeft());
            assertEquals(i + 20000, node.getRight());
        }
    }

    private long generateAndInsert(NodeBlockFactory nodeBlockFactory, KeyNameMapper mapper, int keys) throws JasDBException {
        TreeBlock treeBlock = nodeBlockFactory.createBlock(444, dataBlockFactory.getBlockWithSpace(false));
        long position = treeBlock.getPosition();

        for(int i=0; i<keys; i++) {
            Key key = new StringKey("fieldKey" + i).addKey(mapper, "POINTER", new LongKey(i));
            treeBlock.addKey(new TreeNode(key, i + 10000, i + 20000));
        }

        nodeBlockFactory.persistBlock(treeBlock);
        treeBlock.close();

        return position;
    }
}
