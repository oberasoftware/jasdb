/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package com.oberasoftware.jasdb.core.index.btreeplus.search;

import com.oberasoftware.jasdb.api.exceptions.JasDBStorageException;
import com.oberasoftware.jasdb.core.index.btreeplus.BlockPersister;
import com.oberasoftware.jasdb.core.index.btreeplus.LeaveBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.RootBlock;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockIntentType;
import com.oberasoftware.jasdb.core.index.btreeplus.locking.LockManager;
import com.oberasoftware.jasdb.api.index.keys.Key;
import com.oberasoftware.jasdb.api.index.keys.KeyFactory;
import com.oberasoftware.jasdb.api.index.keys.KeyInfo;
import com.oberasoftware.jasdb.api.index.query.IndexSearchResultIteratorCollection;
import com.oberasoftware.jasdb.core.index.query.IndexSearchResultIteratorImpl;
import com.oberasoftware.jasdb.api.index.query.SearchLimit;
import com.oberasoftware.jasdb.core.index.query.RangeCondition;
import com.oberasoftware.jasdb.api.index.query.SearchCondition;

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
            List<Key> results = new LinkedList<>();
            boolean keepEvaluating = currentLeave.size() > 0;
            while(keepEvaluating && currentLeave != null) {
                addFoundKeys(results, currentLeave.getKeyRange(rangeCondition.getStart(), rangeCondition.isStartIncluded(),
                        rangeCondition.getEnd(), rangeCondition.isEndIncluded()), limit);

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
                currentLeave = nextBlockPointer != -1 ? (LeaveBlock) persister.loadBlock(nextBlockPointer) : null;
                if(currentLeave != null) {
                    lockManager.acquireLock(LockIntentType.READ, currentLeave);
                }
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

//    private Key fillEmptyFields(Key key) throws JasDBStorageException {
//        if(key instanceof CompositeKey) {
//            KeyNameMapper mapper = keyInfo.getKeyNameMapper();
//            //missing fields
//            Key[] keys = key.getKeys();
//            for(String field : keyInfo.getKeyFields()) {
//                int index = mapper.getIndexForField(field);
//                if(keys == null || keys.length <= index || keys[index] == null) {
//                    key.addKey(mapper, field, new AnyKey());
//                }
//            }
//        }
//        return key;
//    }
}
