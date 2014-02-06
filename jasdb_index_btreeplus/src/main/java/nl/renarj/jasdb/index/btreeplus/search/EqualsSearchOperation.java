/*
 * The JASDB software and code is Copyright protected 2012 and owned by Renze de Vries
 * 
 * All the code and design principals in the codebase are also Copyright 2012 
 * protected and owned Renze de Vries. Any unauthorized usage of the code or the 
 * design and principals as in this code is prohibited.
 */
package nl.renarj.jasdb.index.btreeplus.search;

import nl.renarj.jasdb.core.exceptions.JasDBStorageException;
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
import nl.renarj.jasdb.index.search.EqualsCondition;
import nl.renarj.jasdb.index.search.SearchCondition;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Renze de Vries
 */
public class EqualsSearchOperation implements SearchOperation {
    private RootBlock rootBlock;
    private KeyInfo keyInfo;
    private LockManager lockManager;

    public EqualsSearchOperation(LockManager lockManager, RootBlock rootBlock, KeyInfo keyInfo) {
        this.rootBlock = rootBlock;
        this.keyInfo = keyInfo;
        this.lockManager = lockManager;
    }

    @Override
    public IndexSearchResultIteratorCollection search(SearchCondition condition, SearchLimit limit) throws JasDBStorageException {
        EqualsCondition equalsCondition = validateCondition(keyInfo, condition);
        Key desiredKey = equalsCondition.getKey();

        lockManager.startLockChain();
        lockManager.acquireLock(LockIntentType.READ, rootBlock);
        try {
            LeaveBlock leaveBlock = rootBlock.findLeaveBlock(LockIntentType.READ, equalsCondition.getKey());
            return doLeaveSearch(leaveBlock, desiredKey);
        } finally {
            lockManager.releaseLockChain();
        }
    }

    private IndexSearchResultIteratorCollection doLeaveSearch(LeaveBlock leaveBlock, Key desiredKey) {
        List<Key> results = new ArrayList<>(1);

        if(leaveBlock.contains(desiredKey)) {
            results.add(leaveBlock.getKey(desiredKey));
        }

        return new IndexSearchResultIteratorImpl(results, keyInfo.getKeyNameMapper().clone());
    }

    protected static EqualsCondition validateCondition(KeyInfo keyInfo, SearchCondition condition) throws JasDBStorageException {
        if(condition instanceof EqualsCondition) {
            EqualsCondition equalsCondition = (EqualsCondition) condition;
            KeyFactory keyFactory = keyInfo.getKeyFactory();
            if(!keyFactory.supportsKey(equalsCondition.getKey())) {
                Key supportedKey = keyFactory.convertKey(equalsCondition.getKey());

                return new EqualsCondition(supportedKey);
            } else {
                return equalsCondition;
            }
        } else {
            throw new JasDBStorageException("Invalid Equals condition input: " + condition);
        }
    }

}
