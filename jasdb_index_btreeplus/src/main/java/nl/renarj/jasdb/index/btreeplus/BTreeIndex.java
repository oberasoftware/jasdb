/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus;

import com.obera.core.concurrency.ResourceLockManager;
import nl.renarj.core.statistics.StatRecord;
import nl.renarj.core.statistics.StatisticsMonitor;
import nl.renarj.core.utilities.configuration.Configuration;
import nl.renarj.jasdb.core.IndexableItem;
import nl.renarj.jasdb.core.caching.MemoryAware;
import nl.renarj.jasdb.core.exceptions.ConfigurationException;
import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.core.storage.datablocks.DataBlock;
import nl.renarj.jasdb.core.storage.datablocks.DataBlockFactory;
import nl.renarj.jasdb.core.storage.datablocks.impl.DataBlockFactoryImpl;
import nl.renarj.jasdb.index.Index;
import nl.renarj.jasdb.index.IndexHeader;
import nl.renarj.jasdb.index.IndexIterator;
import nl.renarj.jasdb.index.IndexRebuildUtil;
import nl.renarj.jasdb.index.IndexScanReport;
import nl.renarj.jasdb.index.IndexScanner;
import nl.renarj.jasdb.index.IndexState;
import nl.renarj.jasdb.index.ScanIntent;
import nl.renarj.jasdb.index.btreeplus.locking.LockIntentType;
import nl.renarj.jasdb.index.btreeplus.locking.LockManager;
import nl.renarj.jasdb.index.btreeplus.persistence.BlockTypes;
import nl.renarj.jasdb.index.btreeplus.persistence.BtreePlusBlockPersister;
import nl.renarj.jasdb.index.btreeplus.search.EqualsSearchOperation;
import nl.renarj.jasdb.index.btreeplus.search.NotEqualsSearchOperation;
import nl.renarj.jasdb.index.btreeplus.search.RangeSearchOperation;
import nl.renarj.jasdb.index.btreeplus.search.SearchOperation;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.NotEqualsCondition;
import nl.renarj.jasdb.index.search.RangeCondition;
import nl.renarj.jasdb.index.search.SearchCondition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Renze de Vries
 */
public class BTreeIndex implements Index {
    private static final Logger LOG = LoggerFactory.getLogger(BTreeIndex.class);

    private static final String PAGE_SIZE_CONF = "pageSize";

    private static final int DATA_BLOCK_SIZE = 8192;
    private static final int DEFAULT_PAGE_SIZE = 512;

    private FileLock fileLock;

    private KeyInfo keyInfo;
    private final File indexLocation;
    private int pageSize = DEFAULT_PAGE_SIZE;

    private FileChannel channel;
    private RandomAccessFile randomAccess;
    private boolean closed = false;

    private BlockPersister persister;
    private DataBlockFactory dataBlockFactory;

    private RootBlock rootBlock;
    private LockManager lockManager;

    private SearchOperation equalsSearchOperation;
    private SearchOperation rangeSearchOperation;
    private SearchOperation notEqualsSearchOperation;

    private IndexState state;
    private IndexScanReport scanReport;

    private Lock fullLock = new ReentrantLock();
    private AtomicLong recordCount = new AtomicLong(0);
    private ResourceLockManager resourceLockManager = new ResourceLockManager();

    public BTreeIndex(File indexLocation, KeyInfo keyInfo) {
        LOG.debug("Opening index at location: {}", indexLocation);
        this.indexLocation = indexLocation;
        this.keyInfo = keyInfo;
        this.state = IndexState.NOT_INITIALIZED;
    }

    @Override
    public void configure(Configuration configuration) throws ConfigurationException {
        pageSize = configuration.getAttribute(PAGE_SIZE_CONF, DEFAULT_PAGE_SIZE);
    }

    @Override
    public boolean hasUniqueConstraint() {
        return true;
    }

    protected RootBlock getRootBlock() throws JasDBStorageException {
        openIndex();
        return rootBlock;
    }

    protected BlockPersister getPersister() throws JasDBStorageException {
        openIndex();
        return persister;
    }

    protected LockManager getLockManager() throws JasDBStorageException {
        openIndex();
        return lockManager;
    }

    @Override
    public KeyInfo getKeyInfo() {
        return keyInfo;
    }

    @Override
    public String getName() {
        return keyInfo.getKeyName();
    }

    @Override
    public long count() {
        return recordCount.get();
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public int getIndexType() {
        return BtreeIndexHeader.BTREE_INDEX_TYPE_ID;
    }

    @Override
    public int match(Set<String> fields) {
        return keyInfo.match(fields);
    }

    private void initializeIndex() throws JasDBStorageException {
        if(this.rootBlock == null) {
            IndexHeader indexHeader;
            boolean createNew;

            try {
                this.randomAccess = new RandomAccessFile(indexLocation, "rw");
                this.channel = randomAccess.getChannel();
                this.fileLock = this.channel.lock();

                this.dataBlockFactory = new DataBlockFactoryImpl(indexLocation, channel, DATA_BLOCK_SIZE);
                this.dataBlockFactory.open();

                DataBlock headerBlock = dataBlockFactory.getHeaderBlock();

                if(headerBlock.getHeader().marker() == 0) {
                    indexHeader = BtreeIndexHeader.createHeader(headerBlock, pageSize, 0, keyInfo);
                    createNew = true;
                } else {
                    createNew = false;
                    indexHeader = BtreeIndexHeader.loadAndValidateHeader(headerBlock, keyInfo);
                    recordCount.set(indexHeader.count());
                }

                this.persister = new BtreePlusBlockPersister(dataBlockFactory, indexHeader.getPageSize(), keyInfo);
                this.lockManager = persister.getLockManager();
            } catch(FileNotFoundException e) {
                state = IndexState.INVALID;
                throw new JasDBStorageException("Unable to open index", e);
            } catch (IOException e) {
                state = IndexState.INVALID;
                throw new JasDBStorageException("Unable to open index", e);
            } catch(ConfigurationException e) {
                state = IndexState.INVALID;
                throw new JasDBStorageException("Unable to open index", e);
            }

            if(createNew) {
                this.rootBlock = (RootBlock)persister.createBlock(BlockTypes.ROOTBLOCK, -1);
                this.rootBlock.setModified(true);
            } else {
                this.rootBlock = (RootBlock) persister.loadBlock(indexHeader.getHeaderSize());
            }
            rangeSearchOperation = new RangeSearchOperation(lockManager, persister, rootBlock, keyInfo);
            equalsSearchOperation = new EqualsSearchOperation(lockManager, rootBlock, keyInfo);
            notEqualsSearchOperation = new NotEqualsSearchOperation(lockManager, keyInfo, rootBlock, persister);

            state = IndexState.OK;
        }
    }

    @Override
    public IndexSearchResultIteratorCollection searchIndex(SearchCondition searchCondition, SearchLimit searchLimit) throws JasDBStorageException {
        openIndex();

        SearchOperation searchOperation;
        SearchCondition condition = searchCondition;
        if(searchCondition instanceof RangeCondition) {
            searchOperation = rangeSearchOperation;
        } else if(searchCondition instanceof NotEqualsCondition) {
            searchOperation = notEqualsSearchOperation;
        } else if(searchCondition instanceof EqualsCondition){
            condition = handleEqualsToRange((EqualsCondition)searchCondition);
            if(condition == searchCondition) {
                searchOperation = equalsSearchOperation;
            } else {
                searchOperation = rangeSearchOperation;
            }
        } else {
            throw new JasDBStorageException("Search Condition is not supported by Btree");
        }

        StatRecord searchRecord = StatisticsMonitor.createRecord("btree:search");
        resourceLockManager.sharedLock();
        try {
            return searchOperation.search(condition, searchLimit);
        } finally {
            resourceLockManager.sharedUnlock();
            searchRecord.stop();
        }
    }

    private SearchCondition handleEqualsToRange(EqualsCondition equalsCondition) {
        int nrOfKeys = keyInfo.getKeyFields().size();
        if(nrOfKeys > 1) {
            //composite keys always use range search
            return new RangeCondition(equalsCondition.getKey(), true, equalsCondition.getKey(), true);
        } else {
            return equalsCondition;
        }
    }

    @Override
    public IndexIterator getIndexIterator() throws JasDBStorageException {
        openIndex();
        return new FullIndexIterator(rootBlock, lockManager, persister);
    }

    @Override
    public void insertIntoIndex(Key key) throws JasDBStorageException {
        openIndex();

        StatRecord btreeInsertRecord = StatisticsMonitor.createRecord("btree:insert");
        resourceLockManager.sharedLock();
        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.LEAVELOCK_OPTIMISTIC, rootBlock);
        try {
            LeaveBlock leaveBlock = rootBlock.findLeaveBlock(LockIntentType.LEAVELOCK_OPTIMISTIC, key);
            if(leaveBlock.size() == persister.getMaxKeys()) {
                lockManager.releaseLockChain();
                lockManager.startLockChain();
                lockManager.acquireLock(LockIntentType.WRITE_EXCLUSIVE, rootBlock);

                leaveBlock = rootBlock.findLeaveBlock(LockIntentType.WRITE_EXCLUSIVE, key);
                doLeaveBlockInsert(leaveBlock, key);
            } else {
                //no overflow, we can just write into the leave
                doLeaveBlockInsert(leaveBlock, key);
            }
        } finally {
            lockManager.releaseLockChain();
            resourceLockManager.sharedUnlock();
            btreeInsertRecord.stop();
        }
    }

    private void doLeaveBlockInsert(LeaveBlock leaveBlock, Key key) throws JasDBStorageException {
        StatRecord leaveBlockInsert = StatisticsMonitor.createRecord("btree:insert:doBlockInsert");
        try {
            if(!leaveBlock.contains(key)) {
                recordCount.incrementAndGet();
                leaveBlock.insertKey(key);
            } else {
                throw new JasDBStorageException("Key: " + key + " already exists in index");
            }
        } finally {
            leaveBlockInsert.stop();
        }
    }

    @Override
    public void removeFromIndex(Key key) throws JasDBStorageException {
        openIndex();

        resourceLockManager.sharedLock();
        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.LEAVELOCK_OPTIMISTIC, rootBlock);
        try {
            LeaveBlock leaveBlock = rootBlock.findLeaveBlock(LockIntentType.LEAVELOCK_OPTIMISTIC, key);
            if(leaveBlock.size() == persister.getMinKeys()) {
                lockManager.releaseLockChain();
                lockManager.startLockChain();
                lockManager.acquireLock(LockIntentType.WRITE_EXCLUSIVE, rootBlock);
                leaveBlock = rootBlock.findLeaveBlock(LockIntentType.WRITE_EXCLUSIVE, key);

                doLeaveBlockRemove(leaveBlock, key);
            } else {
                doLeaveBlockRemove(leaveBlock, key);
            }
        } finally {
            lockManager.releaseLockChain();
            resourceLockManager.sharedUnlock();
        }
    }

    private void doLeaveBlockRemove(LeaveBlock leaveBlock, Key key) throws JasDBStorageException {
        if(leaveBlock.contains(key)) {
            recordCount.decrementAndGet();
            leaveBlock.removeKey(key);
        } else {
            throw new JasDBStorageException("Key: " + key + " cannot be removed, does not exist in index");
        }
    }

    @Override
    public void updateKey(Key oldKey, Key newKey) throws JasDBStorageException {
        openIndex();

        StatRecord updateIndex = StatisticsMonitor.createRecord("btree:update");
        resourceLockManager.sharedLock();
        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.UPDATE, rootBlock);
        try {
            LeaveBlock leaveBlock = rootBlock.findLeaveBlock(LockIntentType.UPDATE, oldKey);
            doLeaveBlockUpdate(leaveBlock, newKey);
        } finally {
            lockManager.releaseLockChain();
            resourceLockManager.sharedUnlock();
            updateIndex.stop();
        }
    }

    private void doLeaveBlockUpdate(LeaveBlock leaveBlock, Key key) throws JasDBStorageException {
        if(leaveBlock.contains(key)) {
            leaveBlock.updateKey(key);
        } else {
            throw new JasDBStorageException("Unable to update key: " + key + " cannot be found in the index");
        }
    }

    @Override
    public void openIndex() throws JasDBStorageException {
        if(closed) {
            throw new JasDBStorageException("Index is closed");
        } else if(rootBlock == null) {
            fullLock.lock();
            try {
                initializeIndex();
            } finally {
                fullLock.unlock();
            }
        }
    }

    @Override
    public void closeIndex() throws JasDBStorageException {
        openIndex();

        fullLock.lock();
        resourceLockManager.exclusiveLock();
        try {
            if(channel != null) {
                closeIndexResources();
            }
        } finally {
            resourceLockManager.exclusiveUnlock(true);
            fullLock.unlock();
        }
    }

    @Override
    public void removeIndex() throws JasDBStorageException {
        fullLock.lock();
        resourceLockManager.exclusiveLock();
        try {
            if(channel != null) {
                closeIndexResources();
            }
            if(!indexLocation.delete()) {
                indexLocation.deleteOnExit();
            }
        } finally {
            resourceLockManager.exclusiveUnlock(true);
            fullLock.lock();
        }
    }

    private void closeIndexResources() throws JasDBStorageException {
        try {
            if(channel != null) {
                BtreeIndexHeader.createHeader(dataBlockFactory.getHeaderBlock(), pageSize, recordCount.get(), keyInfo);

                persister.close();
                dataBlockFactory.close();

                this.fileLock.release();
                this.channel.close();
                this.randomAccess.close();
                state = IndexState.CLOSED;
                closed = true;
            }
        } catch(IOException e) {
            throw new JasDBStorageException("Unable to cleanly close index", e);
        }
    }

    @Override
    public void flushIndex() throws JasDBStorageException {
        openIndex();
        persister.flush();
    }

    @Override
    public IndexScanReport scan(ScanIntent intent, Iterator<IndexableItem> indexableItems) throws JasDBStorageException {
        try {
            openIndex();
            fullLock.lock();
            try {
                if(intent == ScanIntent.RESCAN || intent == ScanIntent.DETECT_INCOMPLETE || scanReport == null) {
                    scanReport = IndexScanner.doIndexScan(this, keyInfo, indexableItems, intent != ScanIntent.DETECT_INCOMPLETE);
                }
                return scanReport;
            } finally {
                fullLock.unlock();
            }
        } catch(JasDBStorageException e) {
            scanReport = new IndexScanReport(IndexState.INVALID, System.currentTimeMillis(), 0);
            return scanReport;
        }
    }

    @Override
    public void rebuildIndex(Iterator<IndexableItem> indexableItems) throws JasDBStorageException {
        fullLock.lock();
        resourceLockManager.exclusiveLock();
        try {
            resetIndex();

            state = IndexState.REBUILDING;
            IndexRebuildUtil.bulkInsertIndex(this, keyInfo, indexableItems);
            flushIndex();
            state = IndexState.OK;
        } finally {
            resourceLockManager.exclusiveUnlock();
            fullLock.unlock();
        }
    }

    public void resetIndex() throws JasDBStorageException {
        //we close so the old index can be removed and reopened with a new fresh file
        if(!closed) {
            closeIndexResources();
        }

        if(!indexLocation.exists() || indexLocation.delete()) {
            closed = false;
            rootBlock = null;
        } else {
            throw new JasDBStorageException("Unable to reset index, old index could not be removed");
        }
    }

    @Override
    public MemoryAware getMemoryManager() throws JasDBStorageException {
        openIndex();
        return persister;
    }

    @Override
    public IndexState getState() {
        return state;
    }

    @Override
    public String toString() {
        return "BTreeIndex{" +
                "indexLocation=" + indexLocation +
                ", keyInfo=" + keyInfo +
                ", closed=" + closed +
                ", state=" + state +
                '}';
    }
}
