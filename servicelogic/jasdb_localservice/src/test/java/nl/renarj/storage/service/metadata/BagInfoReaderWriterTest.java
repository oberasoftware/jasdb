/*
 * The JASDB software and code is Copyright protected 2011 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2011 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.storage.service.metadata;

import nl.renarj.jasdb.api.metadata.IndexDefinition;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.partitions.BagPartition;
import nl.renarj.jasdb.service.metadata.BagInfoReaderWriter;
import nl.renarj.jasdb.storage.indexing.IndexTypes;
import nl.renarj.jasdb.storage.transactional.TransactionalRecordWriter;
import nl.renarj.storage.DBBaseTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

/**
 * User: Renze de Vries
 * Date: 1/15/12
 * Time: 4:29 PM
 */
public class BagInfoReaderWriterTest extends DBBaseTest {
    private File metadataFile = new File(tmpDir, "testbag.pjsm");

    @After
    public void tearDown() {
        assertDelete(metadataFile);
    }

    @Before
    public void setup() {
        assertDelete(metadataFile);
    }

    @Test
    public void testReadWriteIndex() throws JasDBStorageException {
        BagInfoReaderWriter readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        Set<IndexDefinition> definitions = new HashSet<IndexDefinition>();
        try {
            IndexDefinition btreeIndex = new IndexDefinition("testindex", "testheader", "testvalue", IndexTypes.BTREE.getType());
            IndexDefinition reverseIndex = new IndexDefinition("reverseindex", "reverseheader", "reversevalueheader", IndexTypes.INVERTED.getType());
            definitions.add(btreeIndex);
            definitions.add(reverseIndex);
            
            readerWriter.addIndex(btreeIndex);
            readerWriter.addIndex(reverseIndex);
        } finally {
            readerWriter.closeBagInformation();
        }
        
        readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        try {
            Set<IndexDefinition> indexDefinitions = readerWriter.getIndexes();
            Assert.assertEquals("Expected two index definitions", 2, indexDefinitions.size());
            for(IndexDefinition indexDefinition : indexDefinitions) {
                Assert.assertTrue("The loaded index definition should be in the expected set", definitions.contains(indexDefinition));
            }
        } finally {
            readerWriter.closeBagInformation();
        }
    }

    @Test
    public void testRemoveIndexDefinition() throws JasDBStorageException {
        BagInfoReaderWriter readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        Set<IndexDefinition> definitions = new HashSet<IndexDefinition>();
        try {
            IndexDefinition btreeIndex = new IndexDefinition("testindex", "testheader", "testvalue", IndexTypes.BTREE.getType());
            IndexDefinition reverseIndex = new IndexDefinition("reverseindex", "reverseheader", "reversevalueheader", IndexTypes.INVERTED.getType());
            definitions.add(btreeIndex);
            definitions.add(reverseIndex);

            readerWriter.addIndex(btreeIndex);
            readerWriter.addIndex(reverseIndex);
        } finally {
            readerWriter.closeBagInformation();
        }

        readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        try {
            readerWriter.removeIndex(new IndexDefinition("testindex", "testheader", "testvalue", IndexTypes.BTREE.getType()));
        } finally {
            readerWriter.closeBagInformation();
        }

        readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        try {
            Set<IndexDefinition> indexDefinitions = readerWriter.getIndexes();
            Assert.assertEquals("Expected one index definition", 1, indexDefinitions.size());
            Assert.assertTrue("The loaded index definition should be in the expected set",
                        indexDefinitions.contains(new IndexDefinition("reverseindex", "reverseheader", "reversevalueheader", IndexTypes.INVERTED.getType())));
        } finally {
            readerWriter.closeBagInformation();
        }
    }

    @Test
    public void testReadWritePartitions() throws JasDBStorageException {
        BagInfoReaderWriter readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        Set<BagPartition> partitions = new HashSet<BagPartition>();
        try {
            BagPartition primaryPartition = new BagPartition("randomId1", "local", "primary", "ok", "0", "F", 0);
            BagPartition shadowPartition = new BagPartition("randomId2", "randomremote", "shadow", "ok", "0", "9", 0);
            partitions.add(primaryPartition);
            partitions.add(shadowPartition);
            readerWriter.createOrUpdate(primaryPartition);
            readerWriter.createOrUpdate(shadowPartition);
        } finally {
            readerWriter.closeBagInformation();
        }

        readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        try {
            Set<IndexDefinition> indexDefinitions = readerWriter.getIndexes();
            Set<BagPartition> partitionDefinitions = readerWriter.getPartitions();
            Assert.assertEquals("There should not be any index definitions present", 0, indexDefinitions.size());
            Assert.assertEquals("There should be two partition definitions present", 2, partitionDefinitions.size());
            for(BagPartition partition : partitionDefinitions) {
                Assert.assertTrue("There should be a partition definition in the expected set", partitions.contains(partition));
            }
        } finally {
            readerWriter.closeBagInformation();
        }
    }
    
    @Test
    public void testReadPartitionByRange() throws JasDBStorageException {
        BagInfoReaderWriter readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        Set<BagPartition> partitions = new HashSet<BagPartition>();
        try {
            BagPartition primaryPartition = new BagPartition("randomId1", "somestrategy", "primary", "ok", "0", "7", 0);
            BagPartition shadowPartition = new BagPartition("randomId2", "somestrategy", "shadow", "ok", "8", "F", 0);
            partitions.add(primaryPartition);
            partitions.add(shadowPartition);
            readerWriter.createOrUpdate(primaryPartition);
            readerWriter.createOrUpdate(shadowPartition);
        } finally {
            readerWriter.closeBagInformation();
        }

        readerWriter = new BagInfoReaderWriter("testbag", new TransactionalRecordWriter(metadataFile));
        try {
            BagPartition partition = readerWriter.getPartitionByRange("somestrategy", "0", "7");
            Assert.assertNotNull(partition);
            Assert.assertEquals("PartitionId should be randomId1", "randomId1", partition.getPartitionId());

            partition = readerWriter.getPartitionByRange("somestrategy", "8", "F");
            Assert.assertNotNull(partition);
            Assert.assertEquals("PartitionId should be randomId2", "randomId2", partition.getPartitionId());
        } finally {
            readerWriter.closeBagInformation();
        }
    }
}
