package nl.renarj.jasdb.index.btreeplus;

import junit.framework.Assert;
import nl.renarj.core.utilities.StringUtils;
import nl.renarj.core.utilities.configuration.ManualConfiguration;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.btreeplus.locking.LockManager;
import nl.renarj.jasdb.index.keys.impl.LongKey;
import nl.renarj.jasdb.index.keys.impl.StringKey;
import nl.renarj.jasdb.index.result.IndexSearchResultIterator;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class IndexBaseTest {
	protected static final String RECORD_POINTER = "RECORD_POINTER";

	protected static File tmpDir = new File(System.getProperty("java.io.tmpdir"));
	private static final Logger LOG = LoggerFactory.getLogger(IndexBaseTest.class);

    public static ManualConfiguration createCachingConfiguration(String cacheEnabled, String maxCachedBlocks, String maxCachedSize) {
        Map<String, String> params = new HashMap<>();
        params.put("Enabled", cacheEnabled);

        ManualConfiguration config = new ManualConfiguration("Caching", params);

        if(StringUtils.stringNotEmpty(maxCachedBlocks)) {
            createProperty(config, "MaxCachedBlocks", maxCachedBlocks);
        }

        if(StringUtils.stringNotEmpty(maxCachedSize)) {
            createProperty(config, "MaxCacheMemSize", maxCachedSize);
        }

        return config;
    }

    private static ManualConfiguration createProperty(ManualConfiguration config, String name, String value) {
        Map<String, String> maxCacheBlocksAttribs = new HashMap<>();
        maxCacheBlocksAttribs.put("Value", value);
        maxCacheBlocksAttribs.put("Name", name);

        ManualConfiguration propertyConfig = new ManualConfiguration("Property", maxCacheBlocksAttribs);
        config.addChildConfiguration("Property[@Name='" + name + "']", propertyConfig);

        return propertyConfig;
    }

    public static void cleanData(File dir) {
        if(dir.exists()) {
            LOG.info("Starting cleaning data in directory: {}", dir);
            File[] cleanupFiles = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".pjs") || name.endsWith(".idx") || name.endsWith(".idxm") || name.endsWith(".pjsm") || name.endsWith(".pid");
                }
            });
            LOG.info("Cleaning up {} files", cleanupFiles.length);
            if(cleanupFiles != null) {
                for(File cleanFile : cleanupFiles) {
                    boolean success = cleanFile.delete();
                    LOG.info("Deleting of file: {} successful: {}", cleanFile, success);
                }
            }
        }
    }

    public static void cleanData() {
        cleanData(tmpDir);
    }

    protected static void assertDelete(File deleteFile) {
		if(deleteFile.exists()) {
			Assert.assertTrue(deleteFile.delete());
		}
	}
	
	protected void assertIndexKeysPresent(List<Integer> availableLongKeys, Index index) throws JasDBStorageException {
		for(Integer availableIndex : availableLongKeys) {
			LOG.trace("Checking if key: {} can be found after remove operation", availableIndex);
			IndexSearchResultIterator result = index.searchIndex(new EqualsCondition(new LongKey(availableIndex)), new SearchLimit());
			Assert.assertFalse("There should be a result for: " + availableIndex, result.isEmpty());
		}
	}

    public void doBlockAssert(BlockPersister persister, IndexBlock block, int maxSize, int minBlockSize, long expectedParent) throws JasDBStorageException {
        if(block instanceof RootBlock) {
            RootBlock rootBlock = (RootBlock) block;
            if(!rootBlock.isLeave()) {
                assertTrue(rootBlock.size() >= 1);
            }

            assertTrue(rootBlock.size() <= maxSize);
        } else if(block instanceof TreeBlock) {
            TreeBlock treeBlock = (TreeBlock) block;
            assertTrue("Invalid min block size was: " + treeBlock.size() + " expected min: " + minBlockSize, treeBlock.size() >= minBlockSize);
            assertTrue("Invalid max block size was: " + treeBlock.size() + " expected max: " + maxSize, treeBlock.size() <= maxSize);
            assertEquals("Unexpected parent for tree node", expectedParent, treeBlock.getParentPointer());

            treeBlock.getMin();
        } else {
            LeaveBlockImpl leaveBlock = (LeaveBlockImpl) block;
            assertTrue("Invalid min leave size was: " + leaveBlock.size() + " expected min: " + minBlockSize, leaveBlock.size() >= minBlockSize);
            assertTrue("Invalid max leave size was: " + leaveBlock.size() + " expected max: " + maxSize, leaveBlock.size() <= maxSize);
            assertEquals("Unexpected parent for leave", expectedParent, leaveBlock.getParentPointer());

            if(leaveBlock.getProperties().getNextBlock() != -1) {
                LeaveBlockImpl nextBlock = (LeaveBlockImpl) persister.loadBlock(leaveBlock.getProperties().getNextBlock());
                assertTrue(nextBlock.getFirst().compareTo(leaveBlock.getLast()) > 0);
            }
            if(leaveBlock.getProperties().getPreviousBlock() != -1) {
                LeaveBlockImpl previousBlock = (LeaveBlockImpl) persister.loadBlock(leaveBlock.getProperties().getPreviousBlock());
                assertTrue(previousBlock.getLast().compareTo(leaveBlock.getFirst()) < 0);
            }
        }

        if(block instanceof TreeBlock) {
            TreeBlock treeBlock = (TreeBlock) block;
            boolean isRootAsLeave = false;
            if(treeBlock instanceof RootBlock) {
                isRootAsLeave = ((RootBlock)treeBlock).isLeave();
            }
            for(TreeNode node : treeBlock.getNodes()) {
                if(!isRootAsLeave) {
                    assertTrue("Node should have a left node", node.getLeft() != -1);
                    assertTrue("Node should have a right node", node.getRight() != -1);

                    IndexBlock leftBlock = persister.loadBlock(node.getLeft());
                    IndexBlock rightBlock = persister.loadBlock(node.getRight());
                    assertTrue("Left and right block should not be equal", leftBlock.getPosition() != rightBlock.getPosition());

                    assertTrue("Left block last node: " + leftBlock.getLast() + " should be smaller or equal to: " + node.getKey(), leftBlock.getLast().compareTo(node.getKey()) <= 0);
                    doBlockAssert(persister, leftBlock, maxSize, minBlockSize, treeBlock.getPosition());

                    assertTrue("Right block first node: " + rightBlock.getFirst() + " should be greather than: " + node.getKey(), rightBlock.getFirst().compareTo(node.getKey()) > 0);
                    doBlockAssert(persister, rightBlock, maxSize, minBlockSize, treeBlock.getPosition());
                }
            }
        }
    }

    public void assertBlocks(LockManager lockManager, BlockPersister persister, IndexBlock block, int maxSize, int minBlockSize, long expectedParent) throws JasDBStorageException {
        lockManager.startLockChain();
        try {
            doBlockAssert(persister, block, maxSize, minBlockSize, expectedParent);
        } finally {
            lockManager.releaseLockChain();
        }
    }

    protected void changeAmount(Map<Integer, Integer> amounts, int key, boolean increment) {
        if(!amounts.containsKey(key)) {
            amounts.put(key, 1);
        } else {
            int amount = amounts.get(key);
            if(increment) amount++; else amount--;
            amounts.put(key, amount);
        }
    }

    protected void changeAmount(Map<String, Integer> amounts, String key, boolean increment) {
        if(!amounts.containsKey(key)) {
            amounts.put(key, 1);
        } else {
            int amount = amounts.get(key);
            if(increment) amount++; else amount--;
            amounts.put(key, amount);
        }
    }

    protected long assertIndex(Map<Integer, Integer> keyAmounts, Index index) throws JasDBStorageException {
        long totalSearch = 0;
        for(Map.Entry<Integer, Integer> entry : keyAmounts.entrySet()) {
            long start = System.nanoTime();
            IndexSearchResultIterator results = index.searchIndex(new EqualsCondition(new StringKey("key" + entry.getKey())), new SearchLimit());
            long end = System.nanoTime();
            LOG.debug("Found {} records in {} for key: {}", new Object[]{results.size(), (end - start), "key" + entry.getKey()});
            totalSearch += (end - start);
            assertEquals("Unexpected amount of entities found", entry.getValue(), Integer.valueOf(results.size()));

            assertNotNull(results.next());
        }
        return totalSearch / keyAmounts.size();
    }

}
