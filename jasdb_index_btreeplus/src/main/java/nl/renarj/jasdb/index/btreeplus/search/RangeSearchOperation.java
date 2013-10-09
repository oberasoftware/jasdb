/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus.search;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
import nl.renarj.jasdb.index.btreeplus.BlockPersister;
import nl.renarj.jasdb.index.btreeplus.LeaveBlock;
import nl.renarj.jasdb.index.btreeplus.RootBlock;
import nl.renarj.jasdb.index.btreeplus.locking.LockIntentType;
import nl.renarj.jasdb.index.btreeplus.locking.LockManager;
import nl.renarj.jasdb.index.keys.Key;
import nl.renarj.jasdb.index.keys.factory.KeyFactory;
import nl.renarj.jasdb.index.keys.keyinfo.KeyInfo;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorCollection;
import nl.renarj.jasdb.index.result.IndexSearchResultIteratorImpl;
import nl.renarj.jasdb.index.result.SearchLimit;
import nl.renarj.jasdb.index.search.RangeCondition;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.LinkedList;
import java.util.List;

/**
 * @author Renze de Vries
 * Date: 6/1/12
 * Time: 1:55 PM
 */
public class RangeSearchOperation implements SearchOperation {
    private BlockPersister persister;
    private RootBlock rootBlock;
    private KeyInfo keyInfo;
    private LockManager lockManager;

    public RangeSearchOperation(LockManager lockManager, BlockPersister persister, RootBlock rootBlock, KeyInfo keyInfo) {
        this.rootBlock = rootBlock;
        this.keyInfo = keyInfo;
        this.persister = persister;
        this.lockManager = lockManager;
    }

    @Override
    public IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException {
        RangeCondition rangeCondition = validateRangeCondition(condition);
        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.READ, rootBlock);

        LeaveBlock currentLeave;
        if(rangeCondition.getStart() != null) {
            currentLeave = rootBlock.findLeaveBlock(LockIntentType.READ, rangeCondition.getStart());
        } else {
            currentLeave = rootBlock.findFirstLeaveBlock(LockIntentType.READ);
        }

        try {
            List<Key> results = new LinkedList<Key>();
            boolean keepEvaluating = currentLeave.size() > 0 ? true : false;
            while(keepEvaluating && currentLeave != null) {
                addFoundKeys(results, currentLeave.getKeyRange(rangeCondition.getStart(), rangeCondition.isStartIncluded(), rangeCondition.getEnd(), rangeCondition.isEndIncluded()), limit);

                if(limit.isMaxReached(results.size())) {
                    keepEvaluating = false;
                } else if(rangeCondition.getEnd() != null) {
                    Key lastKey = currentLeave.getLast();
                    int lastKeyCompare = lastKey.compareTo(rangeCondition.getEnd());
                    if(lastKeyCompare > 0) {
                        //this block's last key is bigger or equal to the end condition
                        keepEvaluating = false;
                    }
                }

                long nextBlockPointer = currentLeave.getProperties().getNextBlock();
                currentLeave = nextBlockPointer != -1 ? (LeaveBlock)persister.loadBlock(nextBlockPointer) : null;
                if(currentLeave != null) lockManager.acquireLock(LockIntentType.READ, currentLeave);
            }

            return new IndexSearchResultIteratorImpl(results, keyInfo.getKeyNameMapper().clone());
        } finally {
            lockManager.releaseLockChain();
        }
    }

    private void addFoundKeys(List<Key> outputList, List<Key> foundKeys, SearchLimit limit) {
        int listSize = outputList.size();
        for(Key key : foundKeys) {
            if(!limit.isMaxReached(listSize)) {
                outputList.add(key);
            } else {
                break;
            }
            listSize++;
        }
    }

    private RangeCondition validateRangeCondition(SearchCondition condition) throws JasDBStorageException {
        if(condition instanceof RangeCondition) {
            KeyFactory factory = keyInfo.getKeyFactory();

            RangeCondition rangeCondition = (RangeCondition) condition;
            rangeCondition.setStart(validateKey(factory, rangeCondition.getStart()));
            rangeCondition.setEnd(validateKey(factory, rangeCondition.getEnd()));

            return rangeCondition;
        } else {
            throw new JasDBStorageException("Invalid Range condition input: " + condition);
        }
    }

    private Key validateKey(KeyFactory factory, Key key) throws JasDBStorageException {
        if(key != null && !factory.supportsKey(key)) {
            return factory.convertKey(key);
        } else {
            return key;
        }
    }

}
